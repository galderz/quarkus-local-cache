package org.infinispan.quarkus.hibernate.cache.offheap;

import java.io.*;
import java.nio.charset.Charset;
import java.util.function.Function;

interface Marshalling {

    // TODO see if Hibernate can pass in the bytes directly and avoid any marshalling (use std serialization for now)

    Function<Object, byte[]> marshall();
    Function<byte[], Object> unmarshall();

    Marshalling JAVA = new JavaMarshalling();
    Marshalling UTF8 = new StringMarshalling();

    final class JavaMarshalling implements Marshalling {

        private JavaMarshalling() {
        }

        @Override
        public Function<Object, byte[]> marshall() {
            return obj -> {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try (ObjectOutput oo = new ObjectOutputStream(baos)) {
                    oo.writeObject(obj);
                    return baos.toByteArray();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        @Override
        public Function<byte[], Object> unmarshall() {
            return bytes -> {
                if (bytes == null)
                    return null;

                try (ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                    return oi.readObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            };
        }

    }

    final class StringMarshalling implements Marshalling {

        private static Charset UTF8 = Charset.forName("UTF-8");

        private StringMarshalling() {
        }

        @Override
        public Function<Object, byte[]> marshall() {
            return obj -> obj == null ? null : ((String) obj).getBytes(UTF8);
        }

        @Override
        public Function<byte[], Object> unmarshall() {
            return bytes -> bytes == null ? null : new String(bytes, UTF8);
        }

    }

}
