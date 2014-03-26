import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class TM {// Shitian Ren

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TM tm = new TM();
		System.out.println("Choose mode to run:");
		System.out.println("1 for input from stdin");
		System.out.println("2 for input from file");
		try {
			byte[] input = new byte[500];
			System.in.read(input);
			String in = tm.getStr(input);
			if (in.equals("1")) {
				tm.startFromStdIn();
			} else {
				System.out.println("Please enter file name");
				byte[] infile = new byte[500];
				System.in.read(infile);
				String inf = tm.getStr(infile);
				System.out.println(inf);
				tm.startFromFile(inf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public TM() {
		dm = new DM(this);
	}

	public final static int DataLength = 20;
	public final static int SiteLength = 10;
	public boolean isWait = false;
	HashMap<Integer, Transaction> transactionList = new HashMap<Integer, Transaction>();// tid:transaction
	int time = 0;
	Queue<Integer> waitQueue = new LinkedList<Integer>();
	DM dm = null;

	public void startFromStdIn() {
		while (true) {
			byte[] input = new byte[500];
			try {
				System.in.read(input);
				String inputStr = getStr(input);
				parseLine(inputStr);
			} catch (Exception e) {
				e.printStackTrace();
			}
			time++;
		}
	}

	public void startFromFile(String fileName) {
		File file = new File(fileName);
		try {
			InputStreamReader fs = new InputStreamReader(new FileInputStream(
					file));
			BufferedReader br = new BufferedReader(fs);
			String str = br.readLine();
			while (str != null) {
				parseLine(str);
				str = br.readLine();
				time++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private String getStr(byte[] input) {
		String result = "";
		int i = 0;
		int a = input[i];
		while (a != 13 && a != 10 && i < input.length) {
			result += getChar(a);
			i++;
			a = input[i];
		}
		return result;
	}

	private String getChar(int a) {
		String result = String.valueOf((char) a);
		return result;
	}

	private void parseLine(String line) {
		if (!isWait)
			exeWaitTrans();
		String[] inputArr = line.split(";");

		for (int i = 0; i < inputArr.length; i++) {
			isWait = false;
			String tempCommand = inputArr[i].trim();
			int comNo = parseCommand(tempCommand);
			switch (comNo) {
			case 0:
				System.out.println("unknown operation");
				break;
			case 1:
				beginTran(tempCommand);

				break;
			case 2:
				beginROTran(tempCommand);
				break;
			case 3:
				read(tempCommand);
				break;
			case 4:
				write(tempCommand);
				break;
			case 5:
				dump(tempCommand);
				break;
			case 6:
				end(tempCommand);
				break;
			case 7:
				fail(tempCommand);
				break;
			case 8:
				recover(tempCommand);
				break;
			default:
				System.out.println("error");
			}
		}
	}

	private int parseCommand(String str) {
		if (str.startsWith("begin(")) {
			return 1;
		} else if (str.startsWith("beginRO")) {
			return 2;
		} else if (str.startsWith("R")) {
			return 3;
		} else if (str.startsWith("W")) {
			return 4;
		} else if (str.startsWith("dump")) {
			return 5;
		} else if (str.startsWith("end")) {
			return 6;
		} else if (str.startsWith("fail")) {
			return 7;
		} else if (str.startsWith("recover")) {
			return 8;
		}
		return 0;
	}

	private void recover(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 8; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		int siteNo = Integer.parseInt(outStr);
		if (siteNo > 10 || siteNo < 1) {
			System.out.println("Error: Wrong site number");
			return;
		}
		if (dm.siteList.get(siteNo - 1).siteStatus == SiteStatus.AVAILABLE) {
			System.out.println("Error: Site is up already");
			return;
		}
		dm.siteList.get(siteNo - 1).resetLog();
		dm.siteList.get(siteNo - 1).siteStatus = SiteStatus.AVAILABLE;
		System.out.println("site " + siteNo + " is recovered");
	}

	private void fail(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 5; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		int siteNo = Integer.parseInt(outStr);
		if (siteNo > 10 || siteNo < 1) {
			System.out.println("Error: Wrong site number");
			return;
		}
		for (int i = 1; i <= 20; i++)
			for (int k = 0; k < dm.locksize; k++)
				if (dm.tranId[siteNo][i][k] != -1)
					abort(dm.tranId[siteNo][i][k]);

		dm.clearsLockTable(siteNo);
		dm.siteList.get(siteNo - 1).clearlog();
		dm.siteList.get(siteNo - 1).ReplicatedWhenFail();
		dm.siteList.get(siteNo - 1).siteStatus = SiteStatus.FAIL;
		System.out.println("site " + siteNo + " is failed");
	}

	private void end(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 5; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		int tid = Integer.parseInt(outStr);

		checkTranExist(tid);
		if (transactionList.get(tid).transactionStatus == TransactionStatus.WAITING) {
			abort(tid);
		} else if (transactionList.get(tid).transactionStatus == TransactionStatus.RUNNING) {
			ArrayList<Integer> varids = transactionList.get(tid).trLog;
			for (int i = 0; i < varids.size(); i++) {
				ArrayList<Site> sites = dm.getSitesWithData(varids.get(i));
				for (int k = 0; k < sites.size(); k++) {
					if (sites.get(k).siteStatus == SiteStatus.AVAILABLE) {
						sites.get(k).Commit(varids.get(i));
					}
				}
			}
			releaseLock(tid);
			transactionList.remove(tid);
			System.out.println("Transaction " + tid + " commited");
		} else {
			System.out.println("Transaction " + tid + " is already aborted");
		}
	}

	private void dump(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		boolean dumpVar = false;
		for (int i = 5; i < in.length - 1; i++) {
			if (in[i] == 'x') {
				dumpVar = true;
				continue;
			}
			out[j] = in[i];
			j++;
		}
		if (j == 0) {
			System.out
					.println("dump value of all copies of all variables at all sites");
			checkSites();
			return;
		}
		String outStr = String.valueOf(out).trim();
		int dumpId = Integer.parseInt(outStr);
		if (dumpVar) {
			// dump(xj)
			System.out.println("dump variable of x" + dumpId);
			ArrayList<Site> list = dm.getSitesWithData(dumpId);
			for (int i = 0; i < list.size(); i++) {
				System.out.println("site " + list.get(i).siteId + " data "
						+ dumpId + " val " + list.get(i).getData(dumpId));
			}
		} else {
			// dump(i)
			System.out.println("dump site " + dumpId);
			checkSites(dumpId);
		}
	}

	private void write(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 3; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		String[] outStrArr = outStr.split(",");
		int tid = Integer.parseInt(outStrArr[0].trim());
		String varidS = outStrArr[1];
		String valueStr = outStrArr[2];
		int varid = Integer.parseInt(varidS.substring(1).trim());
		int value = Integer.parseInt(valueStr.trim());
		System.out.println("transaction " + tid + " wants to write variable x"
				+ varid + " to value " + value);
		exeWrite(tid, varid, value);
	}

	private void exeWrite(int tid, int varid, int value) {
		checkTranExist(tid);
		transactionList.get(tid).currentOperation = new Operation(
				OperationType.WRITE, varid, value);

		int recode = dm.Write(tid, varid, value);
		if (recode == 1) {
			waitordie(tid, varid);
		} else if (recode == -1) {
			System.out.println("Transaction " + tid + " is not active");
			System.exit(-1);
		} else if (recode == 0) {
			System.out.println("Successfully writes to log");
			transactionList.get(tid).currentOperation = null;
		} else {// no available sites to write
			wait(tid);
		}
	}

	private void read(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 3; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		String[] outStrArr = outStr.split(",");
		int tid = Integer.parseInt(outStrArr[0].trim());
		String varidS = outStrArr[1];
		int varid = Integer.parseInt(varidS.substring(1).trim());
		System.out.println("transaction " + tid + " wants to read variable x"
				+ varid);
		exeRead(tid, varid);
	}

	private void exeRead(int tid, int varid) {
		checkTranExist(tid);
		transactionList.get(tid).currentOperation = new Operation(
				OperationType.READ, varid);
		int recode = dm.Read(tid, varid);
		if (recode == 1) {
			waitordie(tid, varid);
		} else if (recode == 0) {
			transactionList.get(tid).currentOperation = null;
		} else if (recode == -1) {
			System.out.println("Transaction " + tid + " is not active");
		} else {
			// no available sites for operation
			System.out.println("No available sites, transaction " + tid
					+ " waits");
			wait(tid);
		}
	}

	private void waitordie(int tid, int varid) {
		Transaction cur = getTransaction(tid);
		LockType type = null;
		if (cur == null)
			return;
		ArrayList<Transaction> trans = new ArrayList<Transaction>();
		if (cur.currentOperation.type == OperationType.READ)
			type = LockType.READ;
		else
			type = LockType.WRITE;

		if (type == LockType.READ) {
			trans.addAll(WLTrans(varid));
		}
		if (type == LockType.WRITE) {
			trans.addAll(transactionHaveWOrRLockOnData(varid));
		}
		for (int i = 0; i < trans.size(); i++) {
			Transaction tmp = trans.get(i);
			if ((cur.transactionId != tmp.transactionId)
					&& (cur.timeStamp > tmp.timeStamp)) {
				abort(cur.transactionId);
				return;
			}
		}
		wait(tid);
	}

	private ArrayList<Transaction> transactionHaveWOrRLockOnData(int varid) {
		ArrayList<Transaction> result = new ArrayList<Transaction>();
		ArrayList<Transaction> transactions = getActiveTransactions();
		ArrayList<Site> sites = dm.getSitesWithData(varid);
		for (int i = 0; i < sites.size(); i++) {
			int siteId = sites.get(i).siteId;
			if (dm.sLockTable[siteId][varid] != 0) {
				for (int j = 0; j < dm.locksize; j++) {
					if (dm.tranId[siteId][varid][j] != -1
							&& transactions.contains(transactionList
									.get(dm.tranId[siteId][varid][j])))
						result.add(transactionList
								.get(dm.tranId[siteId][varid][j]));
				}
			}
		}

		if (result.size() == 0) {
			System.out
					.println("Error: no transaction holding write or read lock on data "
							+ varid);
			System.exit(-1);
			return null;
		} else {
			return result;
		}
	}

	private Transaction getTransaction(int tid) {
		Transaction re = transactionList.get(tid);
		return re;
	}

	private ArrayList<Transaction> WLTrans(int varid) {
		ArrayList<Transaction> result = new ArrayList<Transaction>();
		ArrayList<Transaction> transactions = getActiveTransactions();
		ArrayList<Site> sites = dm.getSitesWithData(varid);
		for (int i = 0; i < sites.size(); i++) {
			int siteId = sites.get(i).siteId;
			if (dm.sLockTable[siteId][varid] == -2
					&& transactions.contains(transactionList
							.get(dm.tranId[siteId][varid][0]))) {
				result.add(transactionList.get(dm.tranId[siteId][varid][0]));
			}
		}

		return result;
	}

	public ArrayList<Transaction> getActiveTransactions() {
		ArrayList<Transaction> result = new ArrayList<Transaction>();
		Iterator<Integer> it = transactionList.keySet().iterator();
		while (it.hasNext()) {
			Transaction tran = transactionList.get(it.next());
			if (tran.transactionStatus != TransactionStatus.ABORTED)
				result.add(tran);
		}
		return result;
	}

	private void wait(int tid) {
		checkTranExist(tid);
		System.out.println("Transaction " + tid + " waits");
		transactionList.get(tid).transactionStatus = TransactionStatus.WAITING;
		waitQueue.add(tid);
		isWait = true;
	}

	private void abort(int tid) {
		checkTranExist(tid);
		isWait = false;
		if (transactionList.get(tid).transactionStatus != TransactionStatus.ABORTED) {
			releaseLock(tid);
			transactionList.get(tid).transactionStatus = TransactionStatus.ABORTED;
			System.out.println("Transaction " + tid + " aborted");
		}
	}

	private void releaseLock(int tid) {
		checkTranExist(tid);
		for (int i = 0; i < SiteLength; i++) {
			if (dm.siteList.get(i).siteStatus != SiteStatus.FAIL) {
				for (int m = 1; m <= 10; m++)
					for (int n = 1; n <= 20; n++)
						for (int k = 0; k < dm.locksize; k++)
							if (dm.tranId[m][n][k] == tid) {
								if (dm.sLockTable[m][n] > 0)
									dm.sLockTable[m][n]--;
								else
									dm.sLockTable[m][n] = 0;
								dm.tranId[m][n][k] = -1;
							}
			}
		}
		transactionList.get(tid).clearLockTable();
	}

	private void beginROTran(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 9; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		int tid = Integer.parseInt(outStr);
		if (transactionList.containsKey(tid)) {
			System.out.println("Transaction " + tid + " has existed");
			System.exit(-1);
		}
		System.out.println("Begin Read Only Transaction " + tid);
		Transaction tmpTran = new Transaction(tid, TransactionType.READONLY,
				time);
		transactionList.put(tid, tmpTran);
		createROCopy(tid);
	}

	private void createROCopy(int tid) {
		checkTranExist(tid);
		for (int i = 0; i < SiteLength; i++) {
			Site site = dm.siteList.get(i);
			if (site.siteStatus == SiteStatus.AVAILABLE)
				site.makeROCopy(tid);
		}
	}

	public int ReadOnly(int tid, int varid) {
		if (transactionList.get(tid).transactionStatus != TransactionStatus.RUNNING) {
			return -1;
		}
		ArrayList<Site> sites = dm.getSitesWithData(varid);
		for (int i = 0; i < sites.size(); i++) {
			Site site = sites.get(i);
			if (site.siteStatus == SiteStatus.AVAILABLE) {
				int[] copy = site.getROCopy(tid);
				System.out.println("RO Transaction " + tid + " reads data "
						+ varid + " = " + copy[varid]);
				return 0;
			}
		}
		return -2;// no avaiable sites
	}

	private void beginTran(String inputStr) {
		char[] in = inputStr.toCharArray();
		char[] out = new char[in.length];
		int j = 0;
		for (int i = 7; i < in.length - 1; i++) {
			out[j] = in[i];
			j++;
		}
		String outStr = String.valueOf(out).trim();
		int tid = Integer.parseInt(outStr);
		if (transactionList.containsKey(tid)) {
			System.out.println("Transaction " + tid + " has existed");
			System.exit(-1);
		}
		System.out.println("Begin Read-Write Transaction " + tid);
		Transaction tmpTran = new Transaction(tid, TransactionType.READWRITE,
				time);
		transactionList.put(tid, tmpTran);
	}

	private void exeWaitTrans() {
		for (int i = 0; i < waitQueue.size(); i++) {
			int tid = waitQueue.poll();
			Transaction tran = getTransaction(tid);
			if (tran.transactionStatus != TransactionStatus.WAITING) {
				System.out.println("Error: not waiting transaction");
			} else {
				System.out.println("Transaction " + tid + " tries to resume");
				tran.transactionStatus = TransactionStatus.RUNNING;
				doTrans(tran);
			}
		}
	}

	private void doTrans(Transaction tran) {
		tran.transactionStatus = TransactionStatus.RUNNING;
		if (tran.currentOperation.type == OperationType.READ)
			exeRead(tran.transactionId, tran.currentOperation.dataId);
		else if (tran.currentOperation.type == OperationType.WRITE)
			exeWrite(tran.transactionId, tran.currentOperation.dataId,
					tran.currentOperation.value);
		else {
			System.out.println("Error: Incorrect Operation Type");
			System.exit(-1);
		}
	}

	private void checkTranExist(int tid) {
		if (!transactionList.containsKey(tid)) {
			System.out.println("Transaction " + tid + " does not exist");
			System.exit(-1);
		}
	}

	public void checkSites() {
		ArrayList<Site> siteList = dm.siteList;
		for (int i = 0; i < siteList.size(); i++) {
			System.out.println(siteList.get(i));
		}
	}

	public void checkSites(int siteId) {
		ArrayList<Site> siteList = dm.siteList;
		for (int i = 0; i < siteList.size(); i++) {
			if (siteList.get(i).siteId == siteId) {
				System.out.println(siteList.get(i));
				break;
			}
		}
	}
}
