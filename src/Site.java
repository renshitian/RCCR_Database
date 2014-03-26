import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

public class Site {
	int siteId;
	SiteStatus siteStatus;
	int[] data = new int[21];// int[dataId]=value
	int[] log = new int[21];// int[dataId]=value
	
	Hashtable<Integer, int[]> ROCopies = new Hashtable<Integer, int[]>();
	public HashSet<Integer> replicatedData = new HashSet<Integer>();


	public String toString() {
		String reData = "Data ";
		String reLog = "Log ";
		for (int i = 0; i < 21; i++) {
			if (data[i] != -1)
				reData += (i + ":" + data[i] + " ");
		}
		for (int i = 0; i < 21; i++) {
			if (log[i] != -1)
				reLog += (i + ":" + log[i] + " ");
		}
		return "Site " + siteId + "\n" + reData + "\n";
	}

	public Site(int siteId) {
		this.siteId = siteId;
		this.siteStatus = SiteStatus.AVAILABLE;
		// Even indexed variables are at all sites.
		data[0] = -1;
		for (int i = 1; i <= 20; i++) {
			if (i % 2 == 0) {// dataId is even
				data[i] = 10 * i;
			} else
				data[i] = -1;
		}
		// The odd indexed variables: index%10+1=siteId
		if (siteId % 2 == 0) {
			int index = siteId - 1;
			data[index] = index * 10;
			index = siteId - 1 + 10;
			data[index] = index * 10;
		}
		for (int i = 0; i < 21; i++)
			log[i] = data[i];

	}

	public void clearlog() {
		for (int i = 0; i < 21; i++)
			log[i] = -1;
	}

	public void resetLog() {
		for (int i = 0; i < 21; i++)
			log[i] = data[i];
	}

	public int ReadLog(int dataId) {
		if (log[dataId] == -1) {
			System.out.println("Error: Data " + dataId
					+ " is not in this site!");
			return 0;
		}
		return log[dataId];

	}

	public int ReadDataList(int dataId) {
		if (data[dataId] == -1) {
			System.out.println("Error: Data " + dataId
					+ " is not in this site!");
			return 0;
		}

		return data[dataId];
	}

	public void writeLog(int dataId, int val) {
		if (data[dataId] == -1) {
			System.out.println("Error: Data " + dataId
					+ " is not in this site!");
		}
		log[dataId] = val;

	}

	public int getData(int varid) {
		if (data[varid] == -1)
			return 0;
		return data[varid];

	}

	public void Commit(int dataNums) {//Shitian Ren
		data[dataNums] = log[dataNums];
		RecoverDataWhenWroteCommit(dataNums);
	}

	private void RecoverDataWhenWroteCommit(int dataid) {//Shitian Ren
		if (replicatedData.contains(dataid)) {
			replicatedData.remove(dataid);
		}
	}

	public void ReplicatedWhenFail() {//Shitian Ren
		for (int i = 2; i <= 20; i += 2) {
			replicatedData.add(i);
		}
	}

	public void makeROCopy(int tid) {//Shitian Ren
		if(ROCopies.containsKey(tid)){
			System.out.println("Error: RO Transaction has already have version copy in site "+siteId);
		}
		else{
			ROCopies.put(tid, data.clone());
		}
	}
	
	public int[] getROCopy(int tid){//Shitian Ren
		if(ROCopies.containsKey(tid)){
			return ROCopies.get(tid);
		}
		else{
			System.out.println("Error: no RO copy avaiable");
			return null;
		}
	}
}
