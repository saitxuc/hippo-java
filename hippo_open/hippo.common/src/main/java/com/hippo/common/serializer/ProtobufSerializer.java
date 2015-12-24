package com.hippo.common.serializer;

import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

/**
 * 
 * @author saitxuc
 *
 */
public class ProtobufSerializer implements Serializer
{

    private static final String NEW_BUILDER_METHOD = "newBuilder";

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> byte[] serialize( T obj )
        throws IOException
    {
        checkProtobufMessage( obj.getClass() );

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try
        {
            ( (Message) obj ).writeTo( baos );
        }
        finally
        {
            try
            {
                baos.close();
            }
            catch ( Exception e )
            {
                // close quietly
            }
        }

        return baos.toByteArray();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T deserialize( byte[] source, Class<T> clazz )
        throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
        clazz = checkProtobufMessage( clazz );

        try
        {
            Method newBuilder = clazz.getMethod( NEW_BUILDER_METHOD );

            // fixme no idea ATM how to fix type inference here
            GeneratedMessage.Builder builder = (GeneratedMessage.Builder) newBuilder.invoke( clazz );

            @SuppressWarnings( "unchecked" ) // cast should be safe since it is driven by the type
            T deserialized = (T) builder.mergeFrom( source ).build();

            return deserialized;
        }
        catch ( Throwable t )
        {
            throw new IOException( t );
        }
    }

    private static <T> Class<T> checkProtobufMessage( Class<T> clazz )
    {
        if ( !Message.class.isAssignableFrom( clazz ) )
        {
            throw new IllegalArgumentException( format( "Class %s cannot be serialized via Google Protobuf, it is not a %s",
                                                        clazz.getName(),
                                                        Message.class.getName() ) );
        }
        return clazz;
    }

	@Override
	public String getName() {
		return "protobuf";
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
