package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/** The {@link SimpleVersionedSerializer} for {@link LanceWriteResult}. */
public class LanceWriteResultSerializer implements SimpleVersionedSerializer<LanceWriteResult> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(LanceWriteResult lanceWriteResult) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(lanceWriteResult);
        oos.close();
        return baos.toByteArray();
    }

    @Override
    public LanceWriteResult deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
}
