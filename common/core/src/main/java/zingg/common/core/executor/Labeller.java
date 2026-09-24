package zingg.common.core.executor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import zingg.common.client.ILabelDataViewHelper;
import zingg.common.client.ITrainingDataModel;
import zingg.common.client.ZFrame;
import zingg.common.client.ZinggClientException;
import zingg.common.client.cols.ZidAndFieldDefSelector;
import zingg.common.client.options.ZinggOptions;
import zingg.common.client.util.ColName;
import zingg.common.client.util.ColValues;
import zingg.common.client.util.DFObjectUtil;
import zingg.common.core.preprocess.IPreprocessors;
import zingg.common.core.util.LabellerUtil;

public abstract class Labeller<S,D,R,C,T> extends ZinggBase<S,D,R,C,T> implements IPreprocessors<S,D,R,C,T> {

	public static final Integer QUIT_LABELING = 9;
	public static final Integer INCREMENT = 1;
	private static final long serialVersionUID = 1L;
	protected static String name = "zingg.common.core.executor.Labeller";
	public static final Log LOG = LogFactory.getLog(Labeller.class);
	protected ITrainingDataModel<S, D, R, C> trainingDataModel;
	protected ILabelDataViewHelper<S, D, R, C> labelDataViewHelper;
	
	public Labeller() {
		setZinggOption(ZinggOptions.LABEL);
	}

	public void execute() throws ZinggClientException {
		try {
			LabellerUtil<D, R, C> labellerUtil = new LabellerUtil<D, R, C>();
			LOG.info("Reading inputs for labelling phase ...");
			getTrainingDataModel().setMarkedRecordsStat(getMarkedRecords());
			ZFrame<D,R,C>  unmarkedRecords = getUnmarkedRecords();
			ZFrame<D, R, C> preprocessedUnmarkedRecords = preprocess(unmarkedRecords);
			ZFrame<D,R,C>  updatedLabelledRecords = processRecordsCli(preprocessedUnmarkedRecords);
			//only post processing if there are labelled records
			if(updatedLabelledRecords != null){
				ZFrame<D, R, C> postProcessedLabelledRecords = labellerUtil.postProcessLabel(updatedLabelledRecords, unmarkedRecords);
				getTrainingDataModel().writeLabelledOutput(postProcessedLabelledRecords,args);
			}
			LOG.info("Finished labelling phase");
		} catch (Exception e) {
			throw new ZinggClientException("Error in labelling phase ", e);
		}
	}

	/**
	 * Applies externally collected labels using the same Java-side data path as
	 * the interactive labeller. The caller supplies one decision per cluster;
	 * this method owns validation, selection of the unmarked pair, label-column
	 * update, post-processing, and persistence.
	 *
	 * <p>This is intentionally separate from {@link #execute()}, which is the
	 * stdin-driven CLI flow. It is used by remote APIs such as Spark Connect,
	 * where the client displays the pair and sends the decision back.</p>
	 *
	 * @param labels map of z_cluster to 1 (match), 0 (not a match), or 2 (not sure)
	 * @return number of cluster decisions applied
	 * @throws ZinggClientException if a label is invalid or its cluster is not
	 *         currently unmarked
	 */
	public int applyLabels(Map<String, Integer> labels) throws ZinggClientException {
		if (labels == null || labels.isEmpty()) {
			return 0;
		}

		getTrainingDataModel().setMarkedRecordsStat(getMarkedRecords());
		ZFrame<D, R, C> unmarkedRecords = getUnmarkedRecords();
		if (unmarkedRecords == null) {
			throw new ZinggClientException("No unmarked training pairs found for labeling.");
		}

		ZFrame<D, R, C> preprocessedUnmarkedRecords = preprocess(unmarkedRecords);
		ZFrame<D, R, C> updatedRecords = applyLabelDecisions(preprocessedUnmarkedRecords, labels);
		LabellerUtil<D, R, C> labellerUtil = new LabellerUtil<D, R, C>();
		ZFrame<D, R, C> postProcessed = labellerUtil.postProcessLabel(updatedRecords, unmarkedRecords);
		getTrainingDataModel().writeLabelledOutput(postProcessed, args);
		return labels.size();
	}

	/**
	 * Applies externally supplied decisions to a labelling frame. The frame may
	 * be preprocessed for display/selection, while the caller remains
	 * responsible for post-processing it against the original input frame.
	 */
	protected ZFrame<D, R, C> applyLabelDecisions(ZFrame<D, R, C> lines,
			Map<String, Integer> labels) throws ZinggClientException {
		if (lines == null || lines.isEmpty()) {
			throw new ZinggClientException("No unmarked training pairs found for labeling.");
		}

		ZFrame<D, R, C> updatedRecords = null;
		Set<String> seenClusters = new HashSet<String>();
		for (Map.Entry<String, Integer> entry : labels.entrySet()) {
			String cluster = entry.getKey();
			Integer label = entry.getValue();
			if (cluster == null || cluster.isEmpty()) {
				throw new ZinggClientException("A label decision is missing its z_cluster.");
			}
			if (label == null || (label != ColValues.MATCH_TYPE_NOT_A_MATCH
					&& label != ColValues.MATCH_TYPE_MATCH
					&& label != ColValues.MATCH_TYPE_NOT_SURE)) {
				throw new ZinggClientException("Invalid label '" + label + "' for cluster '" + cluster
						+ "'. Expected 0, 1, or 2.");
			}
			if (!seenClusters.add(cluster)) {
				throw new ZinggClientException("Duplicate label decision for cluster '" + cluster + "'.");
			}

			ZFrame<D, R, C> currentPair = lines.filter(
					lines.equalTo(ColName.CLUSTER_COLUMN, cluster));
			if (currentPair == null || currentPair.isEmpty()) {
				throw new ZinggClientException("Cluster '" + cluster
						+ "' is not present in the current unmarked training pairs.");
			}

			updatedRecords = updateLabelledRecords(label, currentPair, updatedRecords);
		}
		return updatedRecords;
	}

	protected ZFrame<D, R, C> updateLabelledRecords(int label, ZFrame<D, R, C> currentPair,
			ZFrame<D, R, C> updatedRecords) {
		ZFrame<D, R, C> result = getTrainingDataModel().updateRecords(label, currentPair, updatedRecords);
		getTrainingDataModel().updateLabellerStat(label, INCREMENT);
		return result;
	}

	
	public ZFrame<D,R,C> getUnmarkedRecords() {
		ZFrame<D,R,C> unmarkedRecords = null;
		ZFrame<D,R,C> markedRecords = null;
		try {
			unmarkedRecords = getPipeUtil().read(false, false, getModelHelper().getTrainingDataUnmarkedPipe(args));
			try {
				markedRecords = getPipeUtil().read(false, false, getModelHelper().getTrainingDataMarkedPipe(args));
			} catch (Exception e) {
				LOG.warn("No record has been marked yet");
			} catch (ZinggClientException zce) {
					LOG.warn("No record has been marked yet");
			}			
			if (markedRecords != null ) {
				unmarkedRecords = unmarkedRecords.join(markedRecords,ColName.CLUSTER_COLUMN, false,
						"left_anti");
				getTrainingDataModel().setMarkedRecordsStat(markedRecords);
			} 
		} catch (Exception e) {
			LOG.warn("No unmarked record for labelling");
		} catch (ZinggClientException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return unmarkedRecords;
	}

	public ZFrame<D,R,C> processRecordsCli(ZFrame<D,R,C>  lines) throws ZinggClientException {
		LOG.info("Processing Records for CLI Labelling");
		if (lines != null && lines.count() > 0) {
			getLabelDataViewHelper().printMarkedRecordsStat(
					getTrainingDataModel().getPositivePairsCount(),
					getTrainingDataModel().getNegativePairsCount(),
					getTrainingDataModel().getNotSurePairsCount(),
					getTrainingDataModel().getTotalCount()
					);

			lines = lines.cache();
//			List<C> displayCols = getLabelDataViewHelper().getDisplayColumns(lines, args);
			ZidAndFieldDefSelector zidAndFieldDefSelector = new ZidAndFieldDefSelector(args.getFieldDefinition(), false, args.getShowConcise());
			//have to introduce as snowframe can not handle row.getAs with column
			//name and row and lines are out of order for the code to work properly
			//snow getAsString expects row to have same struc as dataframe which is 
			//not happening
			ZFrame<D,R,C> clusterIdZFrame = getLabelDataViewHelper().getClusterIdsFrame(lines);
			List<R>  clusterIDs = getLabelDataViewHelper().getClusterIds(clusterIdZFrame);
			try {
				double score;
				double prediction;
				ZFrame<D,R,C>  updatedRecords = null;
				int selectedOption = -1;
				String msg1, msg2;
				int totalPairs = clusterIDs.size();

				for (int index = 0; index < totalPairs; index++) {
					ZFrame<D,R,C>  currentPair = getLabelDataViewHelper().getCurrentPair(lines, index, clusterIDs, clusterIdZFrame);

					score = getLabelDataViewHelper().getScore(currentPair);
					prediction = getLabelDataViewHelper().getPrediction(currentPair);

					msg1 = getLabelDataViewHelper().getMsg1(index, totalPairs);
					msg2 = getLabelDataViewHelper().getMsg2(prediction, score);
					//String msgHeader = msg1 + msg2;

//					selectedOption = displayRecordsAndGetUserInput(getDSUtil().select(currentPair, displayCols), msg1, msg2);
					selectedOption = displayRecordsAndGetUserInput(currentPair.select(zidAndFieldDefSelector.getCols()), msg1, msg2);
					if (selectedOption == QUIT_LABELING) {
						LOG.info("User has quit in the middle. Updating the records.");
						break;
					}
					updatedRecords = updateLabelledRecords(selectedOption, currentPair, updatedRecords);
					getLabelDataViewHelper().printMarkedRecordsStat(
							getTrainingDataModel().getPositivePairsCount(),
							getTrainingDataModel().getNegativePairsCount(),
							getTrainingDataModel().getNotSurePairsCount(),
							getTrainingDataModel().getTotalCount()
							);
				}
				LOG.warn("Processing finished.");
				return updatedRecords;
			} catch (Exception e) {
				LOG.warn("Labelling error has occured " + e.getMessage());
				throw new ZinggClientException("An error has occured while Labelling.", e);
			}
		} else {
			LOG.info("It seems there are no unmarked records at this moment. Please run findTrainingData job to build some pairs to be labelled and then run this labeler.");
			return null;
		}
	}

	
	protected int displayRecordsAndGetUserInput(ZFrame<D,R,C> records, String preMessage, String postMessage) throws ZinggClientException {
		getLabelDataViewHelper().displayRecords(records, preMessage, postMessage);
		int selection = readCliInput();
		return selection;
	}


	int readCliInput() {
		Scanner sc = new Scanner(System.in);

		while (!sc.hasNext("[0129]")) {
			sc.next();
			System.out.println("Nope, please enter one of the allowed options!");
		}
		String word = sc.next();
		int selection = Integer.parseInt(word);
		// sc.close();

		return selection;
	}

	@Override
	public ITrainingDataModel<S, D, R, C> getTrainingDataModel() {	
		if (trainingDataModel==null) {
			this.trainingDataModel = new TrainingDataModel<S, D, R, C, T>(getContext(), getClientOptions());
		}
		return trainingDataModel;
    }

	public void setTrainingDataModel(ITrainingDataModel<S, D, R, C> trainingDataModel) {
		this.trainingDataModel = trainingDataModel;
	}

	
	public ILabelDataViewHelper<S, D, R, C> getLabelDataViewHelper() {
		if(labelDataViewHelper==null) {
			labelDataViewHelper = new LabelDataViewHelper<S,D,R,C,T>(getContext(), getClientOptions());
			labelDataViewHelper.initVerticalDisplayUtility(getDfObjectUtil());
		}
    	return labelDataViewHelper;
    }

	public void setLabelDataViewHelper(ILabelDataViewHelper<S, D, R, C> labelDataViewHelper) {
		this.labelDataViewHelper = labelDataViewHelper;
	}

	protected abstract DFObjectUtil<S, D, R, C> getDfObjectUtil();
}


