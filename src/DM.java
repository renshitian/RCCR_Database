import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class DM {
	public final static int DataLength = 20;
	public final static int SiteLength = 10;
	public final static int locksize = 20;
	TM TM;
	HashMap<Integer, Transaction> transactionList;
	ArrayList<Site> siteList;
	public int[][] sLockTable = new int[11][21];// sLockTable[siteId][dataId]
												// 0-no lock 1-readLock
												// -2-writelock
	public int[][][] tranId = new int[11][21][locksize];// tranId[siteId][dataId]=transactionId
														// holding this data
	public int[][] repData = new int[11][21];// -1 available 1 not available

	public DM(TM TM) {
		this.TM = TM;
		transactionList = TM.transactionList;
		siteList = new ArrayList<Site>();
		for (int i = 0; i < SiteLength; i++)
			siteList.add(new Site(i + 1));
		for (int i = 0; i <= 10; i++)
			for (int j = 0; j <= 20; j++) {
				sLockTable[i][j] = 0;

				repData[i][j] = -1;
			}
		for (int i = 0; i <= 10; i++)
			for (int j = 0; j <= 20; j++)
				for (int k = 0; k < locksize; k++) {
					tranId[i][j][k] = -1;
				}
	}

	public int Read(int transactionId, int dataId) {
		// New change
		// Handle readonly transactions first
		if (transactionList.get(transactionId).transactionType == TransactionType.READONLY) {
			return TM.ReadOnly(transactionId, dataId);
		}// Handle read-write transaction
		else if (transactionList.get(transactionId).transactionStatus == TransactionStatus.RUNNING) {

			// 1. This transaction has read lock on this data
			if (HasReadLock(transactionId, dataId) != -1) {
				int siteId = HasReadLock(transactionId, dataId);
				readFromDataList(transactionId, dataId, siteId);
				return 0;

			}
			// 2.This transaction has no read lock on this data
			else {
				// 3.1 Some other transaction has write lock on this data
				if (otherTranHoldWriteLockOnData(transactionId, dataId)) {
					System.out.print("Read failure.\n");
					return 1;
					// 3.2 No write lock,can get read lock
				} else {

					int readSite = requireReadLock(transactionId, dataId);
					if (readSite > 0 && readSite <= 10) {
						readFromDataList(transactionId, dataId, readSite);
						// PrintLockTable();
						return 0;
					} else {
						System.out
								.print("ERROR:tmpsite is not in 1-10,obtain write lock fail.");
						return -1;
					}
				}
			}
		}
		return -1;// Not covered by the case
	}

	// 0 succeed, 1 can not require lock---transfer to DM
	public int Write(int transactionId, int dataId, int val) {
		if (transactionList.get(transactionId).transactionType == TransactionType.READWRITE) {
			if (transactionList.get(transactionId).transactionStatus == TransactionStatus.RUNNING) {

				boolean writeLockFlag = checkWriteLockWithAllSites(
						transactionId, dataId);
				// 1. This transaction already has write lock on this data
				if (writeLockFlag) {
					ArrayList<Site> sites = getSitesWithData(dataId);
					writeOnLog(transactionId, dataId, sites, val);
					return 0;
				}
				// 2. Need to require lock on all sites
				else if (this.obtainWriteLockResult(transactionId, dataId)) {
					ArrayList<Site> sites = getSitesWithData(dataId);
					for (int i = 0; i < sites.size(); i++) {
						if (sites.get(i).siteStatus == SiteStatus.FAIL) {
							sites.remove(i);
							i--;
						}

					}
					writeOnLog(transactionId, dataId, sites, val);
					// printX2();
					return 0;

				} else {
					System.out.println("Write Failure");
					return 1;
				}

			} else {
				System.out.print("Transaction " + transactionId
						+ " is not running.\n");
				return -1;// Transaction status is not running}
			}
		} else if (transactionList.get(transactionId).transactionType == TransactionType.READONLY) {
			System.out.println("Error: Read Only Transaction wants to write");
			System.exit(-1);

		}
		return -1;
	}

	public void readLog(int transactionId, int dataId, int siteId) {
		transactionList.get(transactionId).currentOperation = null;
		System.out.println("R(T" + transactionId + ",x" + dataId
				+ ") successful , data " + "value is "
				+ siteList.get(siteId - 1).ReadLog(dataId));

	}

	public void writeOnLog(int transactionId, int dataId,
			ArrayList<Site> sites, int val) {
		transactionList.get(transactionId).wroteTRLog(dataId);
		for (int i = 0; i < sites.size(); i++) {
			// System.out.print("test\n");
			// printX2();
			sites.get(i).writeLog(dataId, val);
			// printX2();
			// System.out.println("variable x" + dataId + " in site " + i
			// + " wrote to " + val + " in log");
		}

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

	// New change
	public ArrayList<Integer> getActiveTransactionIds() {
		ArrayList<Integer> result = new ArrayList<Integer>();
		Iterator<Integer> it = transactionList.keySet().iterator();
		while (it.hasNext()) {
			Transaction tran = transactionList.get(it.next());
			if (tran.transactionStatus != TransactionStatus.ABORTED)
				result.add(tran.transactionId);
		}
		return result;
	}

	public boolean checkAvailability(int dataId, int siteId) {
		return (siteList.get(siteId - 1).siteStatus == SiteStatus.AVAILABLE && !siteList
				.get(siteId - 1).replicatedData.contains(dataId));

	}

	public boolean obtainWriteLockResult(int transactionId, int dataId) {
		// ArrayList<Site> result = new ArrayList<Site>();
		ArrayList<Site> sites = this.getSitesWithData(dataId);
		Site tmpSite;
		for (int i = 0; i < sites.size(); i++) {
			tmpSite = sites.get(i);
			if (tmpSite.siteStatus != SiteStatus.FAIL) {
				// 1.Some other transaction is holding data on this site
				if (siteOtherTranHoldData(tmpSite.siteId, transactionId, dataId)) {

					return false;
				}// 2.Already has write lock on this data
				else if (siteHasWriteLock(tmpSite.siteId, transactionId, dataId)) {
					continue;

				}// 3.get lock
				else {
					executeLock(transactionId, dataId, tmpSite.siteId,
							LockType.WRITE);

				}
			}
		}
		return true;

	}

	public boolean siteHasWriteLock(int siteId, int transactionId, int dataId) {
		return (sLockTable[siteId][dataId] == -2 && tranId[siteId][dataId][0] == transactionId);
	}

	public boolean siteOtherTranHoldData(int siteId, int transactionId,
			int dataId) {

		if (sLockTable[siteId][dataId] == -2
				&& tranId[siteId][dataId][0] != transactionId)
			return true;
		for (int i = 0; i < locksize; i++) {
			if (sLockTable[siteId][dataId] > 0
					&& tranId[siteId][dataId][i] != transactionId)
				return true;
		}

		return false;

	}

	// New change
	public boolean otherTranHoldWriteLockOnData(int transactionId, int dataId) {
		ArrayList<Integer> runningTransactions = getActiveTransactionIds();
		ArrayList<Site> sites = getSitesWithData(dataId);
		for (int i = 0; i < sites.size(); i++) {
			int siteId = sites.get(i).siteId;

			int trId = tranId[siteId][dataId][0];
			if (trId != -1 && sLockTable[siteId][dataId] == -2
					&& runningTransactions.contains(trId)
					&& trId != transactionId)
				return true;
		}
		return false;
		// ArrayList<Transaction> runningTransactions = getActiveTransactions();
		// for (int i = 0; i < runningTransactions.size(); i++) {
		// if (runningTransactions.get(i).transactionId !=
		// transactionId&&checkWriteLockWithAllSites(
		// runningTransactions.get(i).transactionId, dataId))
		// return true;//Other transaction holding write lock on this data
		// }
		// return false;
	}

	public int requireReadLock(int transactionId, int dataId) {
		int tmpSiteId = -1;
		ArrayList<Site> sites = getSitesWithData(dataId);
		for (int i = 0; i < sites.size(); i++) {
			if (checkAvailability(dataId, sites.get(i).siteId)) {
				tmpSiteId = sites.get(i).siteId;
				break;
			}
		}
		if (tmpSiteId == -1) {
			// Wait(transactionId);//no one is available
		}
		// if(tmpSiteId>=0&&tmpSiteId<=10)
		executeLock(transactionId, dataId, tmpSiteId, LockType.READ);

		return tmpSiteId;
	}

	// Check if this transaction has all write locks in all sites
	public boolean checkWriteLockWithAllSites(int transactionId, int dataId) {
		ArrayList<Site> sitesWithData = getSitesWithData(dataId);
		for (int i = 0; i < sitesWithData.size(); i++) {
			if (sitesWithData.get(i).siteStatus != SiteStatus.FAIL
					&& (!transactionList.get(transactionId).hasThisWriteLock(
							sitesWithData.get(i), dataId)))

				return false;

		}
		return true;
	}

	// Find all the sites who has this DataId(DataIndex)
	public ArrayList<Site> getSitesWithData(int dataId) {
		ArrayList<Site> result = new ArrayList<Site>();
		if (dataId % 2 == 0)// Every site has even dataId
			for (int i = 1; i <= 10; i++)
				result.add(siteList.get(i - 1));
		else
			// Odd number
			result.add(siteList.get(dataId % 10 + 1 - 1));
		return result;
	}

	public void executeLock(int transactionId, int dataId, int siteId,
			LockType type) {
		// Site side lock

		if (type == LockType.READ) {
			sLockTable[siteId][dataId]++;
			for (int i = 0; i < locksize; i++)
				if (tranId[siteId][dataId][i] == -1) {
					tranId[siteId][dataId][i] = transactionId;
					break;
				}
		} else {
			sLockTable[siteId][dataId] = -2;
			tranId[siteId][dataId][0] = transactionId;
		}

		// Transaction side lock
		transactionList.get(transactionId).lock(siteId, type, dataId);
		// transactionList.get(transactionId).PrintLockTable();
		// siteList.get(siteId-1).PrintLockTable();
	}

	// If one site has this readlock, return siteId,else return -1
	public int HasReadLock(int transactionId, int dataId) {
		ArrayList<Site> sites = getSitesWithData(dataId);
		for (int i = 0; i < sites.size(); i++) {
			if (sites.get(i).siteStatus != SiteStatus.FAIL) {
				if (transactionList.get(transactionId).hasThisReadLock(
						sites.get(i).siteId, dataId))
					return sites.get(i).siteId;
			}
		}
		return -1;
	}

	public void readFromDataList(int transactionId, int dataId, int siteId) {
		System.out.print("*********************");
		System.out.println("R(T" + transactionId + ",x" + dataId
				+ ") successful and value is "
				+ siteList.get(siteId - 1).ReadDataList(dataId) + " from site "
				+ (siteId - 1));
		// transactionList.get(transactionNum).currentOperation = null;
	}

	public boolean HasWriteLock(int tid, int varid) {
		ArrayList<Site> sites = getSitesWithData(varid);
		for (int i = 0; i < sites.size(); i++) {
			if (sites.get(i).siteStatus != SiteStatus.FAIL
					&& transactionList.get(tid).hasThisWriteLock(sites.get(i),
							varid))

				return true;

		}
		return false;
	}

	public void clearsLockTable(int siteId) {

		for (int j = 0; j <= 20; j++) {
			sLockTable[siteId][j] = 0;
			for (int i = 0; i < locksize; i++)
				tranId[siteId][j][i] = -1;
		}
	}

	/***************************** TEST FUNCTION ************************************/
	// public void printX2(){
	// System.out.print(siteList.get(0).data.get(0).dataId+" : "+siteList.get(0).data.get(0).val);
	// System.out.print(siteList.get(0).data.get(1).dataId+" : "+siteList.get(0).data.get(1).val);
	// }
	public void PrintLockTable() {

		for (int i = 1; i < 11; i++)
			for (int j = 1; j <= 20; j++) {

				System.out.print(i + " ");

				System.out.print(j + " ");
				for (int k = 0; k < 20; k++) {
					System.out.print(tranId[i][j][k] + ",");
				}
				System.out.println();
			}

		System.out.println("---------------");
	}
}
