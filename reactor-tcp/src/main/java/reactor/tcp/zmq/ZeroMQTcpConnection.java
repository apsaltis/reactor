package reactor.tcp.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.function.Fn;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.tcp.AbstractTcpConnection;
import reactor.tuple.Tuple2;

import java.net.InetSocketAddress;

import static reactor.event.selector.Selectors.$;

/**
 * @author Jon Brisbin
 */
public class ZeroMQTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(ZeroMQTcpConnection.class);

	private final Tuple2<Selector, Object> close = $();
	private final    Codec<Buffer, IN, OUT> codec;
	private final    byte[]                 channelId;
	private volatile ZMQ.Socket             socket;

	public ZeroMQTcpConnection(Environment env,
	                           Codec<Buffer, IN, OUT> codec,
	                           Dispatcher ioDispatcher,
	                           Reactor eventsReactor,
	                           byte[] channelId) {
		super(env, codec, ioDispatcher, eventsReactor);
		this.codec = codec;
		this.channelId = channelId;
	}

	@Override
	protected void write(Buffer data, Deferred<Void, Promise<Void>> onComplete, boolean flush) {
		write(data.asBytes(), onComplete, flush);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void write(Object data, Deferred<Void, Promise<Void>> onComplete, boolean flush) {
		socket.sendMore(channelId);
		socket.sendMore("");

		if(data instanceof byte[]) {
			socket.send((byte[])data);
		} else if(data instanceof String) {
			socket.send((String)data);
		} else if(null != codec) {
			Buffer buf = codec.encoder().apply((OUT)data);
			socket.send(buf.asBytes());
		} else {
			throw new IllegalArgumentException("Cannot convert " + data + " into a Buffer.");
		}
	}

	@Override
	protected void flush() {
	}

	@Override
	public void close() {
		super.close();
		eventsReactor.notify(close.getT2(), Event.wrap(null));
	}

	@Override
	public boolean consumable() {
		return true;
	}

	@Override
	public boolean writable() {
		return true;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return null;
	}

	@Override
	public ConsumerSpec on() {
		return new ZeroMQTcpConnectionConsumerSpec();
	}

	void setSocket(ZMQ.Socket socket) {
		synchronized(channelId) {
			this.socket = socket;
		}
	}

	ZMQ.Socket getSocket() {
		synchronized(channelId) {
			return socket;
		}
	}

	private class ZeroMQTcpConnectionConsumerSpec<IN, OUT> implements ConsumerSpec<IN, OUT> {
		@Override
		public ConsumerSpec close(Runnable onClose) {
			eventsReactor.on(close.getT1(), Fn.<Event<IN>>consumer(onClose));
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, Runnable onReadIdle) {
			if(LOG.isInfoEnabled()) {
				LOG.info("readIdle not implemented in ZeroMQTcpConnection.");
			}
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, Runnable onWriteIdle) {
			if(LOG.isInfoEnabled()) {
				LOG.info("writeIdle not implemented in ZeroMQTcpConnection.");
			}
			return this;
		}
	}

}
