# SecondString performance analysis for Zingg

## Scope

This review covers Zingg's current integration with `com.wcohen.ss:secondstring:2021` and the upstream TeamCohen/secondstring implementations that correspond to the algorithms used by Zingg. The focus is throughput, allocation rate, executor memory pressure, cache locality, SIMD/vectorization feasibility, and Spark-level execution behavior.

## Executive summary

The highest-value optimization sequence is:

1. **Fix correctness/semantic hazards before tuning**: validate the apparent `AffineGap` index typo upstream, decide whether `SJaroWinkler` is intentionally Jaro-only, and repair/retire the current bigram tokenizer.
2. **Remove avoidable allocation from the hot loops**: Jaro currently creates lowercased strings, wrappers, mutable copies and common-character strings; Monge-Elkan boxes characters and probes `HashSet`s per DP cell.
3. **Change AffineGap from full memo matrices to rolling primitive rows** for score-only execution. This reduces temporary memory from O(m*n) to O(n) and should materially reduce Spark executor GC pressure.
4. **Improve data layout before applying SIMD**. Compact primitive arrays / character-class tables / integer token IDs are prerequisites for useful vectorization. Jaro itself has irregular dependencies and is a weaker SIMD target than branch-light token intersection and parts of dynamic programming.
5. **Optimize Spark integration after algorithm-level allocation is under control**: reuse immutable distance instances per task/executor, avoid per-row construction, keep scratch state task-local, and measure generated-code/UDF boundaries and serialization overhead.

## Where Zingg uses SecondString

### Dependency

`common/core/pom.xml` depends on `com.wcohen.ss:secondstring:2021`; the root build also references the bundled `thirdParty/lib/secondstring.jar`.

### Direct wrappers and functions

- `SJacc extends com.wcohen.ss.Jaccard`
- `SJaroWinkler extends com.wcohen.ss.Jaro`
- `SAffineGap extends com.wcohen.ss.MongeElkan`
- `BigramJaccard extends com.wcohen.ss.Jaccard`
- `StringSimilarityDistanceFunction` stores an `AbstractStringDistance` and calls `gap.score(first, second)`.

Concrete feature functions instantiate these wrappers, including `JaccSimFunction`, `NumbersJaccardFunction`, `JaroWinklerFunction`, `AJaroWinklerFunction`, `AffineGapSimilarityFunction`, `OnlyAlphabetsAffineGapSimilarity`, `EmailMatchTypeFunction`, and `BigramJaccSimFn`.

## Findings

### P0: probable correctness bug in upstream `AffineGap`

TeamCohen/secondstring currently contains this recurrence term in `AffineGap.MatrixTrio.compute`:

```java
it.get(i=1,j-1) + matchScore
```

The likely intended expression is:

```java
it.get(i-1, j-1) + matchScore
```

The current expression mutates `i` and indexes row `1`. This should be verified against tests/paper semantics before any performance work, because an optimized implementation must not faithfully reproduce an accidental bug unless compatibility explicitly requires it.

### P0: `SJaroWinkler` is Jaro, not Jaro-Winkler

`SJaroWinkler` currently extends `com.wcohen.ss.Jaro`. That means Zingg's `JaroWinklerFunction` and `AJaroWinklerFunction` are using Jaro semantics despite their names. Switching to `com.wcohen.ss.JaroWinkler` would be a behavioral/model change, so this needs a compatibility decision and regression tests rather than a silent fix.

### P0/P1: `BigramTokenizer` is both allocation-heavy and suspicious semantically

The current tokenizer:

- calls `replaceAll("\\s+", "")` for every input, invoking regex machinery and allocating a normalized string;
- concatenates a debug log message before the logger decides whether debug is enabled;
- loops with `cursor < input.length() - 2`;
- adds `input.substring(cursor, cursor + 1)`, which is a one-character substring rather than a bigram;
- maintains interned tokens in a `TreeMap`, giving O(log n) lookup with pointer-heavy traversal.

Before optimizing this path, add tests defining the intended token semantics. If true character bigrams are intended, implement a single-pass tokenizer without regex, use a hash/primitive representation, and avoid materializing token strings where possible.

### P1: Jaro allocates heavily per comparison

The upstream Jaro path does the following for `score(String, String)`:

1. `prepare` lowercases each full input into a new `String`.
2. Each input is wrapped in a `BasicStringWrapper`.
3. `commonChars` creates a `StringBuffer` result and a mutable `StringBuffer` copy of the opposite string.
4. It returns materialized `String` objects for both common-character sequences.
5. A second pass compares the two common strings for transpositions.

For Spark workloads with millions of pair comparisons, these short-lived allocations can dominate GC even when CPU cost per individual string is modest.

#### Proposed implementation

Add an allocation-light Jaro implementation using primitive match-marker arrays:

- no mutable copy of the opposite string;
- no materialized common-character strings;
- count matches and transpositions directly;
- fast ASCII case-folding path;
- exact compatibility fallback to current SecondString for non-ASCII / locale-sensitive cases until equivalence is proven;
- optionally reuse scratch arrays per task/thread for common short-string sizes.

Thread-local/task-local scratch reuse must be bounded to avoid retaining very large arrays after an outlier record.

### P1: AffineGap uses O(m*n) temporary memory when O(n) is enough

`AffineGap.score` constructs three memoized matrices (`m`, `is`, `it`) and subsequently scans the matrix to obtain the best score. Score-only affine-gap DP can be implemented using previous/current rows for the required states.

#### Expected impact

For input lengths `m` and `n`:

- current temporary storage: approximately three O(m*n) double matrices plus object/memo overhead;
- rolling implementation: O(n) primitive doubles, preferably with the shorter string on the column axis;
- fewer allocations and less pointer chasing;
- substantially lower executor peak memory and GC frequency for long fields.

This is likely the single most important memory optimization in the SecondString-dependent path.

### P1: Monge-Elkan character scoring allocates inside the O(m*n) DP

For every non-exact character comparison, `MongeElkan` creates two `Character` objects and probes seven `HashSet`s to determine approximate-match groups.

Replace this with a compact classification table/function. For ASCII, a `byte[128]` or branchless `switch` can map characters to equivalence classes:

- `d/t`
- `g/j`
- `l/r`
- `m/n`
- `b/p/v`
- vowels
- comma/period

Then approximate matching is just `classA != 0 && classA == classB`.

A Unicode-safe fallback can retain current behavior for non-ASCII characters.

### P1: token Jaccard uses object-heavy sets

SecondString's Jaccard converts both strings into `BagOfTokens` and iterates token objects while probing the other bag. For high-volume matching, object/token interning overhead can be significant.

Potential faster representations:

- compact integer token IDs;
- sorted unique integer arrays with two-pointer intersection;
- specialized small-set representation for the common case of very few tokens;
- hash-based primitive sets only past a crossover point.

For many entity-resolution fields, token counts are small enough that sorted primitive arrays can beat hash tables through locality and lower allocation.

## SIMD / Vector API assessment

### Good candidates

1. **Character classification / normalization** after compacting to primitive arrays.
2. **Primitive token intersection / equality scans**, especially for fixed-width integer token IDs.
3. **Some DP arithmetic** (`max`, `add`, threshold checks) after converting AffineGap to rolling primitive rows.

### Poorer candidates

Jaro's matching stage has an irregular search window and match dependencies. SIMD may help only after restructuring, and gains can easily be lower than those from eliminating allocations and branches.

### Important DP caveat

Classic affine-gap recurrences have horizontal/vertical dependencies, so a naive Vector API rewrite will not vectorize an entire row efficiently. Wavefront/striped formulations are possible but add complexity and may only pay off for longer strings. For Zingg's typical short-to-medium entity fields, rolling arrays + cache locality are likely the better first target.

### Recommended SIMD strategy

- establish scalar allocation-light baselines first;
- use JMH with multiple string-length regimes;
- implement Vector API variants behind runtime feature checks;
- retain scalar fallback;
- benchmark on the exact JVM/CPU families used in production (AVX2/AVX-512 differences matter);
- report throughput **and** allocation/GC, not throughput alone.

## Spark-specific optimization opportunities

### 1. Reuse distance-function instances

The distance objects appear effectively immutable for the algorithms Zingg uses after construction. Ensure they are instantiated once per serialized feature function / task rather than per row. Any future scratch buffers should be transient task-local state, not shared mutable singleton state.

### 2. Minimize per-row object materialization

The current `StringSimilarityDistanceFunction.call` returns boxed `Double` and invokes a generic `AbstractStringDistance` interface. Depending on how this function is embedded in Spark, inspect whether this forces a UDF/object boundary that prevents whole-stage code generation.

Where feasible, compare:

- current UDF/function invocation;
- Spark SQL expression/codegen implementation for common similarities;
- vectorized batch execution for Arrow/Pandas-facing paths where applicable.

A native Catalyst expression for the highest-volume metrics could outperform micro-optimizing the Java library because it can avoid row-object conversion and function-dispatch overhead.

### 3. Partition-local bounded scratch buffers

For Jaro/AffineGap fast paths, use task/thread-local scratch arrays with a maximum retained capacity. Reuse arrays across rows in the same task, but discard/replace buffers above a configured threshold to avoid one long string permanently inflating executor memory.

### 4. Length-based early exits and ordering

Before expensive similarity work:

- exact equality is already checked;
- add algorithm-safe length bounds where the final threshold is known;
- execute cheap/high-selectivity features before expensive DP similarities if downstream logic can short-circuit;
- for symmetric algorithms, place the shorter string on the dimension that minimizes working memory.

### 5. Cache only normalized/tokenized values at the right level

Do **not** add an unbounded JVM map from raw string to prepared SecondString wrappers: Spark entity datasets can have very high cardinality and turn such a cache into a leak. Prefer caching normalized/tokenized columns in Spark when those representations are reused across many candidate pairs, so memory is visible to Spark and can participate in partitioning/persistence decisions.

## Benchmark plan

### Microbenchmarks

Use JMH and test at least:

- lengths: 5-20, 20-80, 80-256, 256-1024;
- similarity regimes: identical, one-edit, moderate similarity, dissimilar;
- ASCII names/addresses, punctuation-heavy strings, and Unicode cases;
- metrics: Jaro, MongeElkan/AffineGap, Jaccard, bigram Jaccard.

Report:

- ops/s;
- ns/op p50/p99 where practical;
- bytes allocated/op;
- GC collections/time;
- branch/cache counters with async-profiler/perf when available.

### Spark benchmarks

Use existing Zingg performance datasets/workflows and compare:

- wall-clock feature-generation stage time;
- executor CPU utilization;
- executor GC time;
- peak executor memory;
- shuffle/input size to distinguish compute improvements from pipeline noise;
- records or candidate pairs processed per second.

Run multiple repetitions and isolate the similarity stage where possible.

## Proposed PR sequence

### PR 1: benchmark + correctness guardrails

- Add microbenchmarks for current SecondString-backed functions.
- Add regression tests that pin current Jaro/Jaccard/MongeElkan outputs.
- Add explicit tests/documentation for the `SJaroWinkler` semantic mismatch.
- Add tests defining intended `BigramTokenizer` behavior.

### PR 2: Monge-Elkan / AffineGap fast path

- Implement Zingg-local rolling-row affine-gap scorer if upstream cannot be changed quickly.
- Replace approximate-character hash sets/boxing with compact lookup.
- Preserve current outputs, except where the upstream `i=1` typo is deliberately corrected behind a documented compatibility decision.

### PR 3: allocation-light Jaro

- Primitive marker arrays.
- ASCII fast path + compatibility fallback.
- Bounded task-local scratch reuse.
- Benchmark allocation and throughput.

### PR 4: primitive token Jaccard

- Define tokenization compatibility precisely.
- Use small sorted integer sets / primitive IDs.
- Add crossover benchmark against hash-based representation.

### PR 5: Spark-native execution

- Evaluate a Catalyst/codegen expression for the most frequently used similarity metrics.
- Precompute reusable normalized/tokenized columns when candidate-pair fanout makes it profitable.
- Benchmark end-to-end rather than relying only on JMH.

## Upstream SecondString issue proposal

Title: `Performance/correctness: AffineGap DP typo, O(m*n) matrices, and allocation-heavy hot paths`

Key items:

- verify/fix `it.get(i=1,j-1)`;
- rolling-row AffineGap score implementation;
- table-based MongeElkan char classes;
- allocation-light Jaro;
- JMH benchmark suite;
- primitive token representations.

## Zingg issue proposal

Title: `Performance: reduce SecondString allocations and DP memory in similarity hot paths`

Track the five-PR sequence above, including benchmark thresholds and Spark executor metrics.

## Risk and compatibility notes

- Similarity values are model features; seemingly small numerical/semantic changes can alter trained-model behavior.
- The `SJaroWinkler` mismatch should not be silently corrected in a performance PR.
- Fixing the apparent upstream AffineGap typo may change historical outputs; pin test vectors before deciding migration behavior.
- Unicode case folding must be tested before replacing `String.toLowerCase()` with char-wise ASCII/Unicode shortcuts.
- SIMD should remain an optional optimization unless supported JVM baselines and production CPUs are clearly defined.

## Bottom line

The strongest expected wins are **rolling-row AffineGap**, **eliminating Monge-Elkan boxing/hash lookups**, and **allocation-light Jaro**. These should precede aggressive SIMD work. At Spark scale, the next-order win may come from moving the hottest similarity calculations into Spark-native/code-generated execution and precomputing reusable normalized/tokenized representations rather than repeatedly preparing the same values inside row-level UDF calls.
