import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

/* class to calculate rolling median degree
 * as each input transaction is processed. The output
 * will be a list of median values whose count is
 * equal to number of valid transactions in the
 * input file supplied.
 * A valid transaction means it has an actor, target and
 * a time stamp*/
public class VenmoPaymentEvaluator {
	static File file;
	FileReader fr = null;
	BufferedReader br = null;
	HashMap<String, Integer> actor_target = new HashMap<String, Integer>();
	String elements[] = new String[3];
	Payment p1;
	String[][] calculateTime = new String[10][10];
	HashMap<Integer, LinkedList<Payment>> dictMap = new HashMap<Integer, LinkedList<Payment>>();
	ArrayList<Double> rollingMedian = new ArrayList<Double>();
	Date maximumTimeStamp;
	ArrayList<Integer> medianValues;
	static File f;
	FileOutputStream fos;
	PrintStream out;

	/*
	 * reading line by line of input file to process each transaction and read
	 * actor, target and time stamp values from the json object.
	 */
	void inputParser() throws IOException, ParseException {
		try {

			fr = new FileReader(file);
			br = new BufferedReader(fr);

			String line;
			outer: while ((line = br.readLine()) != null) {
				line = line.substring(line.indexOf("{") + 1, line.indexOf("}"));
				String[] strSplit = line.split(",");
				if (strSplit.length != 3) {
					continue;
				}
				for (int i = strSplit.length - 1; i > 0; i--) {
					if (strSplit[i].split(":").length != 2
							|| strSplit[i].split(":")[1].trim().isEmpty())
						continue outer;
					String element = strSplit[i].split(":")[1]
							.trim()
							.substring(
									1,
									strSplit[i].split(":")[1].trim().length() - 1);
					if (element.isEmpty()) {
						continue outer;
					}
					elements[i] = element;
					if (!actor_target.containsKey(element))
						actor_target.put(element, actor_target.size() + 1);

				}
				if (strSplit[0].split(" ").length != 2)
					continue;
				String timestamp = strSplit[0].split(" ")[1].substring(
						strSplit[0].split(" ")[1].indexOf('"') + 1,
						strSplit[0].split(" ")[1].lastIndexOf('"'));
				if (timestamp.isEmpty()) {
					continue outer;
				}
				elements[0] = timestamp;

				boolean currentPayment = updateTimeFrameWindow(timestamp);
				if (currentPayment) {
					p1 = new Payment(actor_target.get(elements[2]),
							actor_target.get(elements[1]), elements[0]);
					createAndUpdateList();
				} else {
					calculateMedian();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			br.close();
			fr.close();
		}
	}

	/*
	 * If the input transaction is out of order and time frame doesn't fall
	 * within 60 seconds of the maximum time stamp of previous transactions
	 * processed, the same rolling median need to be considered. This method
	 * adds previous rolling median value again to the list of rolling median
	 * values and prints the current rolling median values list"
	 */
	void sameRollingMedianValue() {
		double lastRollingMedianValue = rollingMedian
				.get(rollingMedian.size() - 1);
		rollingMedian.add(lastRollingMedianValue);
		for (int p = 0; p < rollingMedian.size(); p++) {
			System.out.println(rollingMedian.get(p));

		}
	}

	/*
	 * For each input transaction being processed, verify all the transactions
	 * processed till now fall within 60 seconds range either more or less. If
	 * not, remove those transactions in graph and median calculation. This
	 * method helps in writing this scenario.
	 */
	boolean updateTimeFrameWindow(String newTimestamp) throws ParseException {
		SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");
		Date newDate = ft.parse(newTimestamp);
		ArrayList<Integer> removeKeysList = new ArrayList<Integer>();
		ArrayList<Integer> removeElementsList = null;
		if (maximumTimeStamp == null) {
			maximumTimeStamp = ft.parse(newTimestamp);
		} else {
			long maximumTimeStampInSec = maximumTimeStamp.getTime();
			long newTimeStampInSec = newDate.getTime();
			if (maximumTimeStampInSec < newTimeStampInSec)
				maximumTimeStamp = newDate;
		}
		/*
		 * out of order payments which do not fall within 60 frame window of the
		 * maximum time stamp processed till now
		 */
		if ((newDate.getTime() - maximumTimeStamp.getTime()) / 1000 < -59) {
			return false;
		}
		if (dictMap != null) {
			Iterator<Entry<Integer, LinkedList<Payment>>> it = dictMap
					.entrySet().iterator();
			while (it.hasNext()) {
				removeElementsList = new ArrayList<Integer>();
				Map.Entry pair = (Map.Entry) it.next();
				@SuppressWarnings("unchecked")
				LinkedList<Payment> ls = (LinkedList<Payment>) pair.getValue();
				int elementSize = ls.size();
				for (int l = 0; l < elementSize; l++) {
					String oldtimestamp = ls.get(l).getTime();
					Date oldDate;
					try {
						oldDate = ft.parse(oldtimestamp);
						long m2 = newDate.getTime();
						long m1 = oldDate.getTime();
						long diff = (m2 - m1) / 1000;
						if (Math.abs(diff) > 59) {
							removeElementsList.add(l);
						}

					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				if (removeElementsList != null) {
					for (int s = 0; s < removeElementsList.size(); s++) {
						ls.set(removeElementsList.get(s), null);
					}
				}
				int sizeList = ls.size();
				if (ls.size() > 0) {
					for (int f = 0; f < sizeList;) {
						if (ls.get(f) == null) {
							ls.remove(f);
							sizeList--;
							f = 0;
						} else {
							f++;
						}
					}
				}
				if (ls.size() == 0) {
					removeKeysList.add((Integer) pair.getKey());
				} else {
					dictMap.put((Integer) pair.getKey(), ls);
				}
			}
			for (int m = 0; (removeKeysList != null)
					&& m < removeKeysList.size(); m++) {
				dictMap.remove(removeKeysList.get(m));
			}
		}

		return true;
	}

	/*
	 * based on the time stamp value of the current transaction being processed,
	 * transactions already processed are removed if they fall out of range of
	 * 60 seconds. Below method helps in that i.e., updating the list of
	 * transactions to be considered in calculating median value
	 */
	void createAndUpdateList() throws IOException {
		if (dictMap.get(p1.getActor()) == null) {
			LinkedList<Payment> l1 = new LinkedList<Payment>();
			l1.add(p1);
			dictMap.put(p1.getActor(), l1);
		} else {
			LinkedList<Payment> temp = dictMap.get(p1.getActor());
			if (!checkMultipleTransactionBtwSameUsers(p1, temp)) {
				temp.add(p1);
				dictMap.put(p1.getActor(), temp);
			}
		}

		if (dictMap.get(p1.getTarget()) == null) {
			LinkedList<Payment> l1 = new LinkedList<Payment>();
			l1.add(p1);
			dictMap.put(p1.getTarget(), l1);
		} else {
			LinkedList<Payment> temp2 = dictMap.get(p1.getTarget());
			if (!checkMultipleTransactionBtwSameUsers(p1, temp2)) {
				temp2.add(p1);
				dictMap.put(p1.getTarget(), temp2);
			}
		}
		calculateMedian();

	}

	/*
	 * this method checks if the same transaction repeats between 2 users that
	 * falls within the time frame of 60 seconds in which case only time stamp
	 * is updated
	 */

	boolean checkMultipleTransactionBtwSameUsers(Payment p,
			LinkedList<Payment> listOfPayments) {
		boolean status = true;
		boolean multiple = false;
		for (int k = 0; k < listOfPayments.size() && status; k++) {
			if ((p1.getTarget() == listOfPayments.get(k).getTarget())
					&& (p1.getActor() == listOfPayments.get(k).getActor())) {
				multiple = true;
				SimpleDateFormat ft = new SimpleDateFormat(
						"yyyy-MM-dd'T'hh:mm:ss'Z'");
				try {
					Date newDate = ft.parse(p1.getTime());
					Date oldDate = ft.parse(listOfPayments.get(k).getTime());
					if (newDate.getTime() - oldDate.getTime() > 0) {
						listOfPayments.get(k).setTime(p1.getTime());
					}
					status = false;

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			status = false;
		}
		return multiple;
	}

	/*
	 * This method helps in calculating the median of the degrees of all the
	 * nodes of the graph constructed using transactions.
	 */
	void calculateMedian() throws IOException {
		Iterator<Entry<Integer, LinkedList<Payment>>> it = dictMap.entrySet()
				.iterator();
		medianValues = new ArrayList<Integer>();
		while (it.hasNext()) {
			@SuppressWarnings("rawtypes")
			Map.Entry pair = (Map.Entry) it.next();
			@SuppressWarnings("unchecked")
			LinkedList<Payment> ls = (LinkedList<Payment>) pair.getValue();
			medianValues.add(ls.size());
		}
		Collections.sort(medianValues);
		int medianSize = medianValues.size();

		double median;
		if (medianSize % 2 == 0) {
			median = ((double) medianValues.get(medianSize / 2) + (double) medianValues
					.get((medianSize / 2) - 1)) / 2;
		} else {
			median = (double) medianValues.get(medianSize / 2);
		}
		rollingMedian.add(median);
		printOutput();

	}

	/*
	 * this method helps to print the final rolling median values of all the
	 * transactions processed.
	 */
	void printOutput() throws IOException {
		try {
			fos = new FileOutputStream(f);
			PrintStream out = new PrintStream(fos);
			System.setOut(out);
			for (int p = 0; p < rollingMedian.size(); p++) {
				System.out.printf("%.2f %n", rollingMedian.get(p));
				// System.out.println();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			fos.close();
		}

	}

	public static void main(String args[]) throws IOException, ParseException {
		VenmoPaymentEvaluator vp = new VenmoPaymentEvaluator();
		file = new File(args[0]);
		f = new File(args[1]);
		vp.inputParser();
	}
}
