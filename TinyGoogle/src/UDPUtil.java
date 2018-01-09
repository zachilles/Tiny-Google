import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public class UDPUtil {
	private static final int TIMEOUT = 5000;
	private static final int MAXNUM = 5;
	public static final int LENGTH = 40960; // 65507
	public static int delay = 0;
	public static String receive(int port, int remotePort, String remoteIP) throws Exception {
		Thread.sleep(delay);
		String result = "";
		String str_send = port + "";
		byte[] buf = new byte[LENGTH];
		DatagramSocket ds = new DatagramSocket(port);

		DatagramPacket dp_send = new DatagramPacket(str_send.getBytes(), str_send.length(),
				InetAddress.getByName(remoteIP), remotePort);

		DatagramPacket dp_receive = new DatagramPacket(buf, LENGTH);
		ds.setSoTimeout(TIMEOUT);
		int tries = 0;
		boolean receivedResponse = false;

		while (!receivedResponse && tries < MAXNUM) {
			ds.send(dp_send);
			
			
			try {
				ds.receive(dp_receive);
				

				if (!dp_receive.getAddress().equals(InetAddress.getByName(remoteIP))) {
					throw new IOException("Received packet from an umknown source");
				}
				receivedResponse = true;
				if (delay > 0) {
					delay = delay - 100;
				}
			} catch (InterruptedIOException e) {
				tries += 1;
				delay = delay + 100;
				System.out.println("Time out," + (MAXNUM - tries) + " more tries...");
			}
		}

		if (receivedResponse) {
			result = new String(dp_receive.getData(), 0, dp_receive.getLength());
			dp_receive.setLength(LENGTH);
		} else {
			throw new Exception();
		}
		ds.close();
		return result;
	}

	public static void send(String str_send, int port) throws IOException {
		// System.out.println(str_send.getBytes().length);

		int tries = 0;
		
		try {
			if (tries < MAXNUM) {
				byte[] buf = new byte[LENGTH];
				DatagramSocket ds = new DatagramSocket(port);
				DatagramPacket dp_receive = new DatagramPacket(buf, LENGTH);
				boolean f = true;

				while (f) {
					ds.receive(dp_receive);

					String str_receive = new String(dp_receive.getData(), 0, dp_receive.getLength());
					int remotePort = Integer.parseInt(str_receive);

					DatagramPacket dp_send = new DatagramPacket(str_send.getBytes(), str_send.length(),
							dp_receive.getAddress(), remotePort);
					ds.send(dp_send);
					dp_receive.setLength(LENGTH);
					f = false;
				}

				ds.close();
			}
		} catch (Exception e){
			tries++;
			
		}
		
	}
}
