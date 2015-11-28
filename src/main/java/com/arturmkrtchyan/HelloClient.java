package com.arturmkrtchyan;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class HelloClient {
    public static final AttributeKey<CompletableFuture<String>> FUTURE = AttributeKey.valueOf("future");

    private final StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
    private final StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);


    private ChannelPool channelPool;
    private EventLoopGroup eventLoopGroup;

    private void connect(String host, int port,
                         int numberOfThreads, int numberOfConnections) {
        eventLoopGroup = new NioEventLoopGroup(numberOfThreads);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);

        bootstrap.group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .remoteAddress(host, port);


        channelPool = new FixedChannelPool(bootstrap, new AbstractChannelPoolHandler() {
            @Override
            public void channelCreated(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();

                // decoders
                // Add the text line codec combination first,
                pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                pipeline.addLast("stringDecoder", stringDecoder);

                // encoders
                pipeline.addLast("stringEncoder", stringEncoder);

                // business logic handler
                pipeline.addLast("clientHandler", new HelloClientHandler(channelPool));

            }
        }, numberOfConnections);
    }

    private void disconnect() throws Exception {
        eventLoopGroup.shutdownGracefully().sync();
    }

    public CompletableFuture<String> send(final String message) {
        // Sends the message to the server.
        CompletableFuture<String> future = new CompletableFuture<>();

        Future<Channel> f = channelPool.acquire();
        f.addListener(new FutureListener<Channel>() {
            @Override
            public void operationComplete(Future<Channel> f) {
                if (f.isSuccess()) {
                    Channel channel = f.getNow();
                    CompletableFuture<String> previous = channel.attr(HelloClient.FUTURE).getAndSet(future);
                    if (previous != null) {
                        System.err.println("Internal error, completion handler should have been null");
                    }
                    try {
                        channel.writeAndFlush(message, channel.voidPromise());
                    } catch (Exception e) {
                        CompletableFuture<String> current = channel.attr(HelloClient.FUTURE).getAndRemove();
                        channelPool.release(channel);
                        current.completeExceptionally(e);
                    }
                }
            }
        });
        return future;
    }

    public static void main(String[] args) throws Exception {

        HelloClient client = new HelloClient();
        client.connect("localhost", 7657, 1, 10);

        IntStream.range(0, 50).forEach(value -> {
            List<CompletableFuture<String>> futures = Collections.synchronizedList(new LinkedList<>());

            long start = System.currentTimeMillis();

            IntStream.range(0, 1000000).forEach(v -> {
                try {
                    CompletableFuture<String> f = client.send("Hello from client\n");
                    futures.add(f);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

            System.out.println(System.currentTimeMillis() - start);

            CompletableFuture f = CompletableFuture.allOf(futures.toArray(new CompletableFuture[1000000]));
            try {
                f.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
            }
            long end = System.currentTimeMillis();

            System.out.println(end - start);
        });

        client.disconnect();

    }


}


class HelloClientHandler extends SimpleChannelInboundHandler<String> {

    private final ChannelPool channelPool;

    HelloClientHandler(ChannelPool channelPool) {
        this.channelPool = channelPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        //System.out.println(msg);

        Attribute<CompletableFuture<String>> futureAttribute = ctx.channel().attr(HelloClient.FUTURE);
        CompletableFuture<String> future = futureAttribute.getAndRemove();
        //System.out.println(ctx.channel().toString() + Thread.currentThread().getName() + " " + msg);

        //Thread.sleep(100);

        channelPool.release(ctx.channel(), ctx.channel().voidPromise());
        future.complete(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Attribute<CompletableFuture<String>> futureAttribute = ctx.channel().attr(HelloClient.FUTURE);
        CompletableFuture<String> future = futureAttribute.getAndRemove();
        cause.printStackTrace();
        channelPool.release(ctx.channel());
        ctx.close();
        future.completeExceptionally(cause);
    }
}
