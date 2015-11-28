package com.arturmkrtchyan;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;

public class HelloServer {

    private static final int PORT = 7657;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public void start() {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(1);
        try {
            final ServerBootstrap b = new ServerBootstrap();
            b.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
            b.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
            b.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {

                        StringDecoder stringDecoder = new StringDecoder(CharsetUtil.UTF_8);
                        StringEncoder stringEncoder = new StringEncoder(CharsetUtil.UTF_8);
                        HelloServerHandler serverHandler = new HelloServerHandler();

                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            final ChannelPipeline pipeline = ch.pipeline();

                            // decoders
                            pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
                            pipeline.addLast("stringDecoder", stringDecoder);

                            // encoders
                            pipeline.addLast("stringEncoder", stringEncoder);

                            // business logic handler
                            pipeline.addLast("serverHandler", serverHandler);
                        }
                    });

            b.bind(PORT).sync().channel().closeFuture().sync();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            stop();
        }
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }


    public static void main(final String[] args) {
        final HelloServer server = new HelloServer();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
}

@ChannelHandler.Sharable
class HelloServerHandler extends SimpleChannelInboundHandler<String> {

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final String msg) {
        ctx.write("Hello!\n", ctx.channel().voidPromise());
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) {
        ctx.flush();
    }

}
