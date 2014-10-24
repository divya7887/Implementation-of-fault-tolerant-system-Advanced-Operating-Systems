//package replicamain;
import java.io.Serializable;


@SuppressWarnings("serial")
public class Message implements Serializable{

	String type;
	int versionNumber;
	String fileName;
	int sourceId;
	String operationName;
	
	
	public Message(String type, int versionNumber,String fileName, int sourceId, String operationName) {
	
		this.type = type;
		this.versionNumber = versionNumber;
	    this.fileName=fileName;
	    this.sourceId=sourceId;
	    this.operationName=operationName;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getVersionNumber() {
		return versionNumber;
	}
	public void setVersionNumber(int versionNumber) {
		this.versionNumber = versionNumber;
	}

	public int getSourceId() {
		return sourceId;
	}

	public void setSourceId(int sourceId) {
		this.sourceId = sourceId;
	}

	public String getOperationName() {
		return operationName;
	}

	public void setOperationName(String operationName) {
		this.operationName = operationName;
	}
	
	public String getFileName() {
		//System.out.println("Inside get method, filename is : " + fileName);
		return fileName;
	}

	public void setFileName(String file) {
		//System.out.println("Inside set method, filename is : " + fileName);
		this.fileName = file;
	}
	
}
