# SecondString performance analysis for Zingg

## Scope and evidence standard

This review covers Zingg's current integration with the bundled SecondString JAR and the Spark execution path around it. The goal is to distinguish facts that are visible in the code from hypotheses that still need profiling.

The findings below use three evidence levels:

- **Confirmed**: directly established by the current Zingg/SecondString/Spark source or repository history.
- **Mechanically derived**: complexity/allocation consequences that follow directly from those data structures, but are not yet production measurements.
- **Workload-dependent hypothesis**: a plausible optimization whose end-to-end importance must be measured on representative jobs.

This distinction matters. The code is old enough to contain several obvious allocation-heavy designs, but source inspection alone cannot tell us which similarity metric dominates a particular production workload.

## Executive summary

The revised priority is:

1. **Fix confirmed correctness/resource-safety problems first.** Two stand out: Spark UDF registration can alias different functions under the same name, and SecondString's default Jaccard tokenizer keeps a process-wide, mutable, unbounded token-interning `TreeMap`.
2. **Measure the existing fuzzy path before globally ranking algorithms.** Zingg's existing FEBRL 120K and NC Voters 5M performance jobs already exercise Monge-Elkan/AffineGap and Jaro heavily, so they are suitable A/B harnesses.
3. **Treat AffineGap rolling rows as a high-confidence per-call memory optimization, not yet as the globally largest production win.** Its current storage cost is mechanically large and can be reduced from O(m*n) to O(n), but total job impact depends on how many fuzzy comparisons are executed and what else dominates the Spark stage.
4. **Treat Spark's per-feature Java UDF boundary as a first-class target.** Zingg registers each similarity separately as `UDF2` and calls it through `functions.callUDF`; Spark 3.5.5's generated code must convert/box values around Java UDF calls and cannot inline the similarity implementation into Catalyst.
5. **Modernize data layout before considering explicit SIMD.** Zingg currently compiles for Java 11, while `jdk.incubator.vector` is unavailable on that baseline and is still incubating in current newer JDKs. Primitive, allocation-light scalar loops should come first; HotSpot auto-vectorization should be measured before introducing a separate Vector-API build path.

No production-wide speedup ranking is claimed until allocation/CPU profiles are captured.

---

## 1. What artifact does Zingg actually run?

### Confirmed: `2021` is a synthetic local Maven version

Zingg does not resolve a public upstream `com.wcohen.ss:secondstring:2021` release. The root build installs:

```text
thirdParty/lib/secondstring.jar
```

into the local Maven repository and assigns it the coordinates:

```text
com.wcohen.ss:secondstring:2021
```

The bundled JAR is currently 201,564 bytes with Git blob SHA:

```text
92ddf6c9a48e010e9bf4e748956bf52c89a67b9b
```

Repository history shows that this JAR was added in the initial Zingg repository import on 2021-08-25 and has not subsequently changed.

### Strong provenance evidence, but not byte-for-byte proof

SourceForge's June 2012 SecondString release is `secondstring-20120620.jar` (reported as 201.2 kB). The last TeamCohen source commit before that release is:

```text
a332794d08b8bf420abb2a0607eaf9c8d0a0d95f
2012-05-18
```

The `AffineGap.java` blob at that historical revision is byte-identical at the source-file level to current TeamCohen `master` (`829cb55f...`) and already contains the `i=1` expression discussed below.

This makes the June-2012 lineage much more relevant than current GitHub HEAD alone. However, the bundled Zingg JAR has **not** been byte-compared with the SourceForge JAR in this review, so the report does not claim exact binary identity.

### Action

Before changing score semantics, add one reproducible artifact-provenance check to the build or release documentation:

- checksum the bundled JAR;
- identify the exact upstream release/source revision it came from;
- ideally replace the synthetic `2021` coordinate with a documented vendored-artifact version/checksum.

---

## 2. Verified runtime call map

| Zingg match path | Zingg implementation | SecondString runtime path | Status |
|---|---|---|---|
| `FUZZY` string | `AffineGapSimilarityFunction` | `SAffineGap -> MongeElkan -> AffineGap` | active |
| `FUZZY` string | `JaroWinklerFunction` | `SJaroWinkler -> Jaro` | active |
| `TEXT` string | `JaccSimFunction` | `SJacc -> Jaccard -> SimpleTokenizer.DEFAULT_TOKENIZER` | active |
| `NUMERIC` string | `NumbersJaccardFunction` | constructs `SJacc`, but `call()` does not use it | active Zingg code, dead SecondString scorer |
| `EMAIL` string | `EmailMatchTypeFunction` | `SAffineGap -> MongeElkan -> AffineGap` after local-part extraction | active |
| `ONLY_ALPHABETS_FUZZY` | `OnlyAlphabetsAffineGapSimilarity` | `SAffineGap -> MongeElkan -> AffineGap` after filtering | active |
| `BigramJaccSimFn` | `BigramJaccard` | `Jaccard` with custom tokenizer | class exists but is not wired by current `StringFeature` |

### The AffineGap path is definitely reachable

For a `FUZZY` string field, `StringFeature.addSimFunctionsForFuzzyString()` adds both:

```java
new AffineGapSimilarityFunction();
new JaroWinklerFunction();
```

`AffineGapSimilarityFunction` creates `SAffineGap`; `SAffineGap` extends `MongeElkan`; and `MongeElkan.score()` calls `super.score(...)`, i.e. `AffineGap.score()`.

The earlier analysis was wrong to leave this call chain implicit. What remains unknown is **how much of a production job's wall time it accounts for**, not whether the code is reached.

### Existing performance workloads exercise it heavily

The existing repository performance configs are useful here:

- FEBRL 120K has **eight** `FUZZY` string fields. Each candidate pair therefore executes eight Monge-Elkan/AffineGap UDF calls and eight Jaro UDF calls.
- NC Voters 5M has **two** `FUZZY` string fields.

The latest stored reports (2026-06-28) show match durations of 5.82 minutes for FEBRL 120K and 52.43 minutes for NC Voters 5M. Those numbers are end-to-end baselines only; they do not attribute time to SecondString.

---

## 3. Confirmed correctness and resource-safety findings

### P0: Spark UDF registration can alias different implementations

`NumbersJaccardFunction` currently calls:

```java
super("JaccSimFunction");
```

which gives it the same logical function name as `JaccSimFunction`.

`SparkTransformer` currently uses `function.getName()` as its UDF registration identifier, and `SparkFnRegistrar` deliberately skips registration when that identifier already exists:

```java
if (!sparkSession.catalog().functionExists(functionName)) {
    sparkSession.udf().register(functionName, udf2, dataType);
}
```

Therefore, if a model contains both TEXT Jaccard and NUMERIC Jaccard, whichever implementation registers first under `JaccSimFunction` remains registered and the other transformer calls the same already-registered UDF.

`ModelUtil` uses a `LinkedHashMap`, so this is deterministic with field-definition order rather than random, but it is still semantically wrong for one of the fields.

#### Recommended fix

Decouple the internal Spark UDF identifier from the public feature/function name. A low-risk option is to use the already-unique generated output column (`z_sim_<count>`) as the registration ID while leaving output-column naming and model feature semantics unchanged.

This should be fixed independently of performance benchmarking.

---

### P0/P1: default SecondString Jaccard has process-wide unbounded interning state

SecondString's default Jaccard constructor uses:

```java
SimpleTokenizer.DEFAULT_TOKENIZER
```

which is a static singleton. That tokenizer contains:

```java
private int nextId = 0;
private Map tokMap = new TreeMap();
```

Every previously unseen token is added to `tokMap` and never removed.

Zingg's `SJacc` has no explicit constructor, so `Jaccard()` selects this singleton. `JaccSimFunction` constructs `SJacc` for TEXT similarity.

#### Consequences

These are structural, not speculative:

1. **Retained memory grows with vocabulary cardinality, not with one comparison.** Every unique token seen by the executor/classloader remains strongly referenced.
2. **Lookup cost grows as O(log V)** because token interning uses `TreeMap`, where `V` is all distinct tokens seen so far by that tokenizer.
3. **The state is shared and mutable.** `TreeMap` is not thread-safe. Zingg's current performance tests use Spark `local[*]`, and normal Spark executors also execute multiple task threads in one JVM, so a process-wide mutable tokenizer is an unsafe design for parallel matching.
4. **Jaccard builds more state than it needs.** `BagOfTokens` creates another `TreeMap` per input and allocates boxed `Double` counts, even though plain Jaccard only needs distinct-token membership and cardinalities.

This finding changes the optimization roadmap materially. For high-cardinality TEXT workloads, replacing this Jaccard path can improve both boundedness and throughput and may be more important than AffineGap.

#### Recommended Zingg-side replacement

Implement a compatibility-preserving Jaccard scorer that:

- tokenizes letter runs and digit runs with the same semantics as `SimpleTokenizer(true, true)`;
- ignores punctuation;
- treats duplicate tokens as one set element;
- uses per-comparison compact state with no global interning;
- uses a small-array representation for the common low-token-count case and a primitive/open-addressed representation only past a measured crossover;
- initially falls back to legacy SecondString for non-ASCII/locale-sensitive cases if exact equivalence is not proven.

For an ASCII fast path, token slices can be hashed/compared case-insensitively without materializing token `String`s. This would remove the global vocabulary map entirely.

---

### P0 compatibility question: the AffineGap `i=1` expression predates the 2012 release

The source revision immediately preceding the June 2012 release contains:

```java
it.get(i=1,j-1) + matchScore
```

rather than the likely intended:

```java
it.get(i-1,j-1) + matchScore
```

That historical file has the same blob SHA as current TeamCohen `master`, so this is not a recent GitHub regression.

The expression assigns `1` to local `i` before the lookup. It is highly suspicious and inconsistent with the neighboring recurrence terms, but this review still does **not** assert that Zingg's bundled binary has been bytecode-verified to contain it.

#### Action before a fast implementation

- add score regression vectors against the actual bundled JAR;
- independently implement the mathematically intended recurrence;
- compare outputs over randomized strings;
- decide whether a corrected algorithm needs a new feature/version name rather than silently changing historical model features.

---

### P0 compatibility question: `SJaroWinkler` is actually Jaro

`SJaroWinkler` extends `com.wcohen.ss.Jaro`, not `JaroWinkler`.

This means Zingg's `JaroWinklerFunction` naming does not match its current score semantics. It should be documented and tested, but not silently changed in a performance PR because similarity values are ML features.

---

## 4. AffineGap / Monge-Elkan: confirmed per-call memory problem

### Current representation

Each `MemoMatrix` allocates both:

```java
double[][] value;
boolean[][] computed;
```

for `(m + 1) * (n + 1)` cells.

`AffineGap.MatrixTrio` constructs **three** such matrices: the main matrix and the two gap-state matrices.

On HotSpot, looking only at array payloads (8-byte doubles and approximately one byte per boolean) and ignoring row/object headers and alignment, the storage is approximately:

```text
3 * (m + 1) * (n + 1) * 9 bytes
```

before any other scorer allocation.

For equal-length inputs:

| Length | Matrix payload estimate/call | Six rolling double rows |
|---:|---:|---:|
| 20 | ~11.6 KiB | ~1.0 KiB |
| 50 | ~68.6 KiB | ~2.4 KiB |
| 80 | ~173.0 KiB | ~3.8 KiB |
| 256 | ~1.70 MiB | ~12.0 KiB |

The current implementation allocates the full arrays even though `MemoMatrix` computes cells lazily. `AffineGap.score()` then scans every main-matrix cell to find the best score, so the main matrix is fully demanded anyway.

### Mechanically justified optimization

A score-only affine-gap recurrence can keep rolling primitive rows for the required states. This changes temporary storage from O(m*n) to O(min(m,n)) while preserving double arithmetic.

This is a **high-confidence per-call memory improvement**. Whether it is the largest end-to-end Zingg improvement is workload-dependent.

### Implementation notes

- Put the shorter string on the row-array dimension where recurrence semantics permit.
- Keep the existing explanation/full-matrix path separate if `explainScore()` needs the matrix.
- Do not mix the recurrence-correction decision with the memory-layout change; benchmark a compatibility mode and a mathematically corrected mode separately.
- Use flat primitive arrays, not nested arrays.

---

## 5. Monge-Elkan character scoring: remove object/hash work from the DP cell

For a non-exact character pair, the historical/current code constructs two `Character` objects and probes seven `HashSet`s to discover approximate-match classes.

A compact classifier can represent the same ASCII groups directly:

- `d/t`
- `g/j`
- `l/r`
- `m/n`
- `b/p/v`
- `a/e/i/o/u`
- comma/period

For example, a guarded ASCII table can map chars to small class IDs:

```java
if (c < 128 && d < 128) {
    int cc = CLASS_TABLE[c];
    int dc = CLASS_TABLE[d];
    ...
}
```

The guard is essential; indexing a 128-entry table with arbitrary Java `char` values is invalid.

A non-ASCII fallback can preserve existing `Character.toLowerCase` behavior.

This removes hashing and pointer chasing from an O(m*n) inner loop. The actual CPU gain still needs measurement because HotSpot may scalar-replace some short-lived objects after inlining.

---

## 6. Jaro: allocation-heavy source path, but lowercasing was overstated previously

The earlier version of this report said `toLowerCase()` necessarily creates new strings. That is too strong.

What is confirmed by source:

- `AbstractStringDistance.score(String, String)` calls `prepare()` for both inputs.
- `Jaro.prepare()` invokes `String.toLowerCase()` and wraps each result in `BasicStringWrapper`.
- `String.toLowerCase()` can return the original object when no character changes; allocation therefore depends on the input/JDK/locale behavior.
- Each `commonChars()` call creates a result `StringBuffer` and a mutable `StringBuffer` copy of the opposite string, then materializes a result string.
- `commonChars()` is called in both directions, followed by another pass to count transpositions.

So there is a real allocation/locality hypothesis, but the **actual bytes/op must be measured** because lowercasing may allocate zero, one, or two strings and HotSpot escape analysis may eliminate some temporary objects.

### Proposed implementation after measurement

An allocation-light Jaro fast path can use primitive match-marker arrays and direct transposition counting. Start with ASCII-compatible behavior and retain legacy fallback for non-ASCII/locale-sensitive cases until differential tests demonstrate equivalence.

Bound any reused scratch arrays so one outlier long string does not permanently enlarge per-thread executor state.

---

## 7. Zingg-side preprocessing has independent hot-path waste

Several active functions do expensive work before SecondString is entered.

### `NumbersJaccardFunction`

Confirmed source behavior per call includes:

- `Pattern.compile("\\d+")`;
- two new `HashSet<String>` instances;
- `Matcher` objects and matched-substring materialization;
- several debug-message string concatenations even when DEBUG logging is disabled;
- construction of an `SJacc` scorer in the constructor even though `call()` never uses `gap`.

At minimum, the pattern should be `static final` and debug concatenation should be guarded. A manual digit-run scanner can go further, but should be benchmarked against the simple static-Pattern change.

### `OnlyAlphabetsAffineGapSimilarity`

It calls `replaceAll("[0-9.]", "")` on both inputs before the DP scorer. A single-pass filter can preserve the exact ASCII-digit/dot semantics and return the original string when there is nothing to remove, avoiding regex work and often avoiding allocation entirely.

### `EmailMatchTypeFunction`

It calls `split("@", 0)` on both emails but consumes only element zero. Modern JDKs have a fast path for simple one-character `String.split` delimiters, so this should not be described as full regex compilation on every call. It still creates a result array and domain substrings that are immediately discarded. `indexOf('@')` plus a prefix slice/string avoids that unnecessary work.

These cleanups are low-risk candidates for a separate PR because they need not change SecondString's score algorithm.

---

## 8. Spark execution is a confirmed optimization surface

### One Java UDF per similarity feature

`SparkModel` constructs one `SparkTransformer` per `SimFunction`.

Each transformer registers a Java `UDF2<T,T,Double>` and applies it with:

```java
functions.callUDF(uid, leftColumn, rightColumn)
```

For a fuzzy field, AffineGap and Jaro are therefore two independent UDF expressions over the same pair of strings.

### Spark 3.5.5 generated-code behavior

Spark 3.5.5's `ScalaUDF.doGenCode` explicitly treats Java UDFs as the untyped case. Generated code:

- converts non-primitive Catalyst inputs through converter functions;
- boxes primitive inputs where relevant;
- calls the UDF through a function object (`udf.apply(...)`);
- converts/unboxes the result back to Catalyst representation.

For StringType, this means the similarity implementation does not operate directly on Catalyst `UTF8String`; the Java UDF receives Java objects/strings. Catalyst cannot inline or reason about the internals of Jaro/AffineGap/Jaccard.

### Optimization experiments to benchmark

#### A. Fuse same-field similarities into one Java UDF

For a FUZZY field, one composite UDF could:

- convert the left/right strings once;
- perform shared cheap checks once (`null`, empty, equality, lengths);
- optionally share normalization;
- return both base similarities.

This keeps the stable public algorithms while reducing repeated UDF/conversion overhead. The return-container allocation cost must be included in the benchmark.

#### B. Compute the base feature vector in one pass

A more aggressive experiment is a single feature-vector transformer that computes all configured base similarities before `PolynomialExpansion`, instead of one `withColumn(callUDF(...))` per feature.

Potential benefits:

- one row boundary instead of N Java UDF boundaries;
- fewer repeated Java String conversions;
- easier scratch-buffer reuse;
- direct production of the vector consumed by the ML pipeline.

Risks include saved-model compatibility, feature observability, and a larger refactor.

#### C. Catalyst-native expressions for the hottest metrics

A custom Catalyst expression can avoid generic Java-UDF conversion and potentially operate on `UTF8String` directly. This offers the highest integration ceiling but couples Zingg to Spark internal APIs, so it should only follow profiling that proves the UDF boundary is material.

#### D. Precompute reusable normalization selectively

If one source record fans out to many candidate pairs, repeatedly lowercasing/filtering/tokenizing the same value is wasteful. Precomputing normalized columns before pair expansion can help, but increases row width and shuffle/persistence cost. Use a fanout-aware benchmark rather than adding a JVM-global cache.

---

## 9. SIMD / Vector API: lower priority under the current Java baseline

Zingg currently compiles with:

```text
maven.compiler.source = 11
maven.compiler.target = 11
```

and the existing performance workflow explicitly runs Temurin Java 11.

The JDK Vector API was introduced after Java 11 and remains an incubating module in modern JDKs (including JDK 25). Therefore a direct `jdk.incubator.vector` implementation is **not compatible with Zingg's current Java-11 build contract**.

Real options would be:

1. raise the Java baseline;
2. ship a separate/multi-release optional implementation compiled on a newer JDK and dispatch at runtime;
3. use JNI/native code, with significantly higher packaging/operational cost.

None should precede the simpler primitive-layout work.

### Near-term SIMD strategy

- write flat primitive scalar loops first;
- inspect whether HotSpot C2 auto-vectorizes normalization/classification loops;
- use JMH + `perf`/JFR/assembly inspection to identify actual vectorization opportunities;
- only introduce explicit Vector API code when a supported JVM profile and measurable gain justify the maintenance cost.

Jaro's matching windows and affine-gap dependencies remain irregular SIMD targets. Character classification, ASCII scanning, hashing, and some primitive-array operations are more promising.

---

## 10. Measurement plan that resolves the remaining ranking uncertainty

### Step 1: capture allocation + CPU attribution on the existing perf workloads

Use the existing Java-11 `local[*]` performance jobs and add an opt-in profiling mode. JFR is available on the existing Temurin 11 runtime and can report:

- allocation hot spots;
- GC pressure;
- CPU samples;
- lock/contention signals;
- thread activity.

Capture at least:

- FEBRL 120K match phase (8 fuzzy fields);
- NC Voters 5M match phase (2 fuzzy fields).

Keep the current end-to-end minute-based regression reports as the final acceptance metric, but do not use wall time alone to choose what to optimize.

### Step 2: microbenchmark isolated scorers

JMH cases should include:

- Jaro;
- MongeElkan/AffineGap;
- Jaccard;
- Zingg numeric Jaccard;
- wrapper preprocessing.

Dimensions:

- lengths 5-20, 20-80, 80-256, 256-1024;
- lowercase vs mixed case;
- ASCII vs Unicode;
- identical, one-edit, moderate-similarity, dissimilar;
- vocabulary cardinality for Jaccard, including long-running tests that expose the global intern map.

Report:

- throughput / ns-op;
- bytes allocated/op;
- GC events/time;
- retained heap after many unique Jaccard tokens;
- scalability under concurrent threads.

### Step 3: A/B structural Spark experiments

Measure separately:

1. baseline per-feature UDFs;
2. unique UDF-registration IDs only (correctness fix; should be perf-neutral);
3. fused same-field fuzzy UDF;
4. rolling-row AffineGap;
5. allocation-light Jaro;
6. replacement Jaccard without global interning;
7. combinations of the above.

This will show interaction effects and prevent attributing Spark-boundary wins to algorithm changes.

---

## 11. Revised PR sequence

### PR A — correctness: make Spark UDF registration identity unique

- decouple registration ID from `SimFunction.getName()`;
- preserve existing feature/output-column names;
- add regression coverage for TEXT + NUMERIC Jaccard in one model.

### PR B — profiling/benchmark guardrails

- opt-in JFR artifact for existing performance workflows;
- JMH similarity benchmark module or focused benchmark harness;
- regression vectors against the bundled SecondString binary.

### PR C — low-risk Zingg preprocessing cleanup

- static `Pattern` for numeric Jaccard;
- guarded debug logging;
- remove dead `SJacc` construction from numeric Jaccard where API-safe;
- non-regex digit/dot filtering for only-alphabet fuzzy matching;
- local-part extraction without `split` array creation.

### PR D — bounded/local Jaccard implementation

- remove dependency on `SimpleTokenizer.DEFAULT_TOKENIZER` from Zingg's TEXT hot path;
- no global token retention;
- concurrency tests;
- compatibility/differential tests;
- benchmark small-array vs primitive-hash representations.

### PR E — rolling-row Monge-Elkan/AffineGap

- O(min(m,n)) working storage;
- compact char-class lookup;
- compatibility mode against the bundled JAR;
- separate decision for the historical recurrence anomaly.

### PR F — allocation-light Jaro

- primitive match markers;
- conditional/bounded scratch reuse;
- ASCII fast path only after differential testing;
- preserve existing Jaro semantics despite the `JaroWinklerFunction` name.

### PR G — fused Spark feature execution

- first experiment with same-field FUZZY fusion;
- then consider direct feature-vector construction or Catalyst-native expressions if profiling supports it.

### PR H — explicit SIMD only if still justified

Only after primitive scalar versions and JVM baseline decisions are complete.

---

## 12. Bigram code: real defects, currently low runtime priority

`BigramJaccSimFn`/`BigramJaccard` contains suspicious behavior (one-character substrings despite the name, `TreeMap` interning, regex whitespace removal, and questionable token-array construction), but current `StringFeature` does not wire this function into normal match-type selection.

It should receive correctness tests or be retired before reuse, but it should not be presented as a current production hot path without evidence that another extension/configuration instantiates it.

---

## Bottom line

The most important new conclusion is not a micro-optimization: **SecondString Jaccard's default tokenizer is process-wide mutable state with unbounded token retention**, and Zingg's Spark UDF registration can make different similarity implementations alias under one function name. Those are confirmed defects and should be addressed before speculative SIMD work.

For FUZZY workloads, the AffineGap memory layout remains a compelling optimization because its per-call array payload is mechanically large and rolling rows reduce it by orders of magnitude. Existing Zingg performance jobs already execute that path heavily, so the next step is to capture allocation/CPU profiles there and let measurements decide whether rolling DP, Jaro allocation reduction, or Spark UDF fusion produces the largest end-to-end gain.
