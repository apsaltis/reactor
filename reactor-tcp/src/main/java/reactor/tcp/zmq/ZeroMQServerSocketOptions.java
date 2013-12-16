package reactor.tcp.zmq;

import org.zeromq.ZContext;
import reactor.tcp.config.ServerSocketOptions;

/**
 * @author Jon Brisbin
 */
public class ZeroMQServerSocketOptions extends ServerSocketOptions {

	private ZContext ctx;

	public ZeroMQServerSocketOptions context(ZContext ctx) {
		this.ctx = ctx;
		return this;
	}

	public ZContext context() {
		return this.ctx;
	}

}
