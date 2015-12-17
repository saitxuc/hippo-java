package com.hippo.common.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;


/**
 * 
 * @author saitxuc
 * 2015-1-26
 */
public class StandardSerializer implements Serializer
{

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( obj );
        oos.flush();
        oos.close();
        return baos.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, final Class<T> clazz )
        throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis = new ByteArrayInputStream( source );
        ObjectInputStream ois = new ObjectInputStream( bis )
        {

            @Override
            protected Class<?> resolveClass( ObjectStreamClass objectStreamClass )
                throws IOException, ClassNotFoundException
            {
                ClassLoader classLoader = clazz.getClassLoader();
                return classLoader != null
                    ? classLoader.loadClass( objectStreamClass.getName() )
                    : Class.forName( objectStreamClass.getName() );
            }

        };
        T obj = clazz.cast( ois.readObject() );
        ois.close();
        return obj;
    }

	@Override
	public String getName() {
		return "standard";
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
