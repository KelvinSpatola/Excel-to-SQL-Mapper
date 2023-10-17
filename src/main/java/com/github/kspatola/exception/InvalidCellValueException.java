package com.github.kspatola.exception;

public class InvalidCellValueException extends RuntimeException {

	@java.io.Serial
	private static final long serialVersionUID = 2998893866610641024L;

	private String invalidText;
	
	public InvalidCellValueException() {
        super();
    }
	
	public InvalidCellValueException(String invalidText) {
		this(null, null, invalidText);
	}

    public InvalidCellValueException(String message, String invalidText) {
    	this(message, null, invalidText);
    }

    public InvalidCellValueException(String message, Throwable cause, String invalidText) {
        super(message, cause);
        this.invalidText = invalidText;
    }
    
    public String getIvalidCellValue() {
    	return invalidText;
    }

}
