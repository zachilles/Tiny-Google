
public class HelperToken {
	private String server = "";
	private int port = 0;
	private int thread = 0;
	
	public HelperToken(String server, int port, int thread) {
		this.server = server;
		this.port = port;
		this.thread = thread;
	}

	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getThread() {
		return thread;
	}

	public void setThread(int thread) {
		this.thread = thread;
	}
	
	
	
}
