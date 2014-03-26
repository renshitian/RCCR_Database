public class Lock {
	public int siteId;
	public int numOfReadLock;
	public int numOfWriteLock;
	public int dataId;
   
	public Lock(int siteId,int dataId){
		this.dataId=dataId;
		this.siteId=siteId;
		this.numOfReadLock=0;
		this.numOfWriteLock=0;
	}
	
	public String toString(){
		return "Lock: "+dataId+" in "+siteId+" No.R "+numOfReadLock+" No.W "+numOfWriteLock;
	}
}
