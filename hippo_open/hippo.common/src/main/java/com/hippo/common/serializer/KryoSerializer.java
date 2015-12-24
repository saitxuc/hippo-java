package com.hippo.common.serializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoSerializer  implements Serializer, Closeable
{
    /* buffer size */
    private static final int BUFFER_SIZE = 1024;

    private final KryoPool pool;

    public KryoSerializer(KryoPool pool)
    {
        this.pool = pool;
    }

    public KryoSerializer()
    {
        this(new KryoPool());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize(T obj)
            throws IOException
    {
        Class<?> clazz = obj.getClass();

        KryoHolder kh = null;
        try
        {
            kh = pool.get();
            kh.reset();
            //checkRegiterNeeded(kh.kryo, clazz);

            //kh.kryo.writeObject(kh.output, obj);
            kh.kryo.writeClassAndObject(kh.output, obj);
            return kh.output.toBytes();
        }
        finally
        {
            if (kh != null)
            {
                pool.done(kh);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        KryoHolder kh = null;
        try
        {
            kh = pool.get();
            //checkRegiterNeeded(kh.kryo, clazz);

            Input input = new Input(source);
            //return kh.kryo.readObject(input, clazz);
            return (T) kh.kryo.readClassAndObject(input);
        }
        finally
        {
            if (kh != null)
            {
                pool.done(kh);
            }
        }
    }

    /**
     * Closes the pool releasing any associated Kryo instance with it
     * @throws IOException
     */
    @Override
    public void close() 
    {
        pool.close();
    }

    private static void checkRegiterNeeded(Kryo kryo, Class<?> clazz)
    {
        kryo.register( clazz );
    }



    private static class KryoHolder
    {
        final Kryo kryo;
        final Output output = new Output(BUFFER_SIZE, -1);

        KryoHolder(Kryo kryo)
        {
            this.kryo = kryo;
        }

        private void reset()
        {
            output.clear();
        }
    }

    public static class KryoPool
    {
        private final Queue<KryoHolder> objects = new ConcurrentLinkedQueue<KryoHolder>();

        public KryoHolder get()
        {
            KryoHolder kh;
            if ((kh = objects.poll()) == null)
            {
                kh = new KryoHolder(createInstance());
            }
            return kh;
        }

        public void done(KryoHolder kh)
        {
            objects.offer(kh);
        }

        public void close()
        {
            objects.clear();
        }

        /**
         * Sub classes can customize the Kryo instance by overriding this method
         *
         * @return create Kryo instance
         */
        protected Kryo createInstance()
        {
            Kryo kryo = new Kryo();
            kryo.setMaxDepth(20);
            kryo.setRegistrationRequired(false);
            kryo.setReferences(false);
            return kryo;
        }

    }

	@Override
	public String getName() {
		return "kryo";
	}

}
