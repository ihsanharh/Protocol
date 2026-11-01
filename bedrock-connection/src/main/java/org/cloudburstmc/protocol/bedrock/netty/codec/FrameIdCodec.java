package org.cloudburstmc.protocol.bedrock.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import org.cloudburstmc.netty.channel.raknet.RakReliability;
import org.cloudburstmc.netty.channel.raknet.packet.RakMessage;
import org.cloudburstmc.protocol.bedrock.netty.BedrockBatchWrapper;

import java.util.List;

@Sharable
public class FrameIdCodec extends MessageToMessageCodec<Object, BedrockBatchWrapper> {
    public static final String NAME = "frame-id-codec";
    private final int frameId;

    public FrameIdCodec(int frameId) {
        this.frameId = frameId;
    }

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof RakMessage || msg instanceof ByteBuf;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, BedrockBatchWrapper msg, List<Object> out) throws Exception {
        if (msg.getCompressed() == null) {
            throw new IllegalStateException("Bedrock batch was not compressed");
        }

        CompositeByteBuf buf = ctx.alloc().compositeDirectBuffer(2);
        try {
            buf.addComponent(true, ctx.alloc().ioBuffer(1).writeByte(frameId));
            buf.addComponent(true, msg.getCompressed().retainedSlice());

            out.add(buf.retain());
        } finally {
            buf.release();
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
        ByteBuf in;
        if (msg instanceof RakMessage rakMsg) {
            if (rakMsg.channel() != 0 && rakMsg.reliability() != RakReliability.RELIABLE_ORDERED) {
                return;
            }
            in = rakMsg.content();
        } else if (msg instanceof ByteBuf buf) {
            in = buf;
        } else {
            return;
        }

        if (!in.isReadable()) {
            return;
        }
        int id = in.readUnsignedByte();
        if (id != frameId) {
            throw new IllegalStateException("Invalid frame ID: " + id);
        }

        out.add(BedrockBatchWrapper.newInstance(in.readRetainedSlice(in.readableBytes()), null));
    }
}
