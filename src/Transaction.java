import java.util.ArrayList;

public class Transaction {
	int transactionId;// initiation covered by new Transaction();
	TransactionType transactionType;// initiation covered by new Transaction();
	TransactionStatus transactionStatus;// initiation covered by new
										// Transaction();
	int timeStamp;// initiation covered by new Transaction();
	public ArrayList<Integer> trLog = new ArrayList<Integer>();
	public int[][] trLockTable=new int[11][21];//trLockTable[siteId][dataId]=-1- no lock 1-readlock 2-wrtelock
	Operation currentOperation;
	int[] ROVersion = new int[20];
	boolean[] ROVAvailale = new boolean[20];
	public Transaction(int transactionId, TransactionType transactionType,
			int timeStamp) {
		this.transactionId = transactionId;
		this.transactionType = transactionType;
		this.timeStamp = timeStamp;
		this.transactionStatus = TransactionStatus.RUNNING;
		for(int i=0;i<=10;i++)
			for(int j=0;j<=20;j++){
				trLockTable[i][j]=-1;
			}

		
		for(int i = 0 ; i < 20 ; i++){
			ROVAvailale[i]=false;
		}
	}

	public boolean hasThisWriteLock(Site site, int dataId) {
	
		return (trLockTable[site.siteId][dataId]==2);
			
	}

	public void lock(int siteId, LockType lockType, int dataId) {
		if(lockType==LockType.READ)
			trLockTable[siteId][dataId]=1;
		else
			trLockTable[siteId][dataId]=2;

	}

	public boolean hasThisReadLock(int siteId, int dataId) {
		return trLockTable[siteId][dataId]==1;
	}

	public void wroteTRLog(int dataNum) {
		if (!trLog.contains(dataNum))
			trLog.add(dataNum);
	}
    public void clearLockTable(){
    	for(int i=0;i<=10;i++)
    		for(int j=0;j<=20;j++)
    			trLockTable[i][j]=-1;
    }
    public String toString() {
		String re = "TRAN " + transactionId + " Type " + transactionType
				+ " COP " + currentOperation + " Status " + transactionStatus
				+ " timeStamp " + timeStamp;
		return re;
	}
	public void PrintLockTable(){
		
		for (int i = 1; i < 11; i++)
		    for(int j=1;j<=20;j++)
		         if(trLockTable[i][j]!=-1){
		    	
			    System.out.print(i+" ");
				
				System.out.print(j+" ");
				System.out.print(trLockTable[i][j]+" ");
				
			System.out.println();
		}
		System.out.println("---------------");
	}

}
