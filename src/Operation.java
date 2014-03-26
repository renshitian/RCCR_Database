public class Operation {
	OperationType type;
	int dataId;
	int value;

	public Operation(OperationType type, int dataNum, int value) {
		this.type = type;
		this.dataId = dataNum;
		this.value = value;
	}

	public Operation(OperationType type, int dataNum) {
		this.type = type;
		this.dataId = dataNum;
	}

	public String toString() {
		return "OP type " + type + " dataNum " + dataId + " value " + value;
	}

}
