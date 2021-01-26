package com.ctrip.flink.hermes;

import com.ctrip.hermes.core.message.BaseConsumerMessage;
import com.ctrip.hermes.core.message.codec.MessageCodecVersion;
import com.ctrip.hermes.core.transport.netty.Magic;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Created by zyxing on 2020/12/9
 */
public class HermesDecoder implements Deserializer {

	@Override
	public void configure(Map map, boolean b) {

	}

	@Override
	public byte[] deserialize(String topic, byte[] bytes) {
		ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
		Magic.readAndCheckMagic(byteBuf);
		byte versionByte = byteBuf.readByte();
		MessageCodecVersion version = MessageCodecVersion.valueOf(versionByte);
		try {
			BaseConsumerMessage msg = version.getHandler().decode(topic, byteBuf, String.class);

			return msg.getBody().toString().getBytes();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void close() {

	}
}
