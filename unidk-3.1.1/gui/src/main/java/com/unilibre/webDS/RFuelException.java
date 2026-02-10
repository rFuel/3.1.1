package com.unilibre.webDS;

public class RFuelException extends RuntimeException {

	private final int status;

	public RFuelException(int status, String msg) {
		super(msg);
		this.status = status;
	}

	public int getStatus() {
		return status;
	}
}
