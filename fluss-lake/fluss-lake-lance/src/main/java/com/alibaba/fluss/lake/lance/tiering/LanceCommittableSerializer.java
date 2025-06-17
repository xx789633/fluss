package com.alibaba.fluss.lake.lance.tiering;

import com.alibaba.fluss.lake.serializer.SimpleVersionedSerializer;

import com.lancedb.lance.FragmentMetadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/** The serializer of {@link LanceCommittable}. */
public class LanceCommittableSerializer implements SimpleVersionedSerializer<LanceCommittable> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(LanceCommittable lanceCommittable) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(lanceCommittable.committable());
        oos.close();
        return baos.toByteArray();
    }

    @Override
    public LanceCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new UnsupportedOperationException(
                    "Expecting LanceCommittable version to be "
                            + CURRENT_VERSION
                            + ", but found "
                            + version
                            + ".");
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        ObjectInputStream ois = new ObjectInputStream(bais);
        try {
            return new LanceCommittable((List<FragmentMetadata>) ois.readObject());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
