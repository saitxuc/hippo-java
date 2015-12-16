package com.pinganfu.hippo.common.hessian.io;

import java.io.IOException;
import java.util.*;

/**
 * 
 * @author sait.xuc
 * Date: 13/9/29
 * Time: 15:10
 *
 */
public class CollectionDeserializer extends AbstractListDeserializer {
	private Class _type;

	public CollectionDeserializer(Class type) {
		_type = type;
	}

	public Class getType() {
		return _type;
	}

	public Object readList(AbstractHessianInput in, int length)
			throws IOException {
		Collection list = createList();

		in.addRef(list);

		while (!in.isEnd())
			list.add(in.readObject());

		in.readEnd();

		return list;
	}

	public Object readLengthList(AbstractHessianInput in, int length)
			throws IOException {
		Collection list = createList();

		in.addRef(list);

		for (; length > 0; length--)
			list.add(in.readObject());

		return list;
	}

	private Collection createList() throws IOException {
		Collection list = null;

		if (_type == null)
			list = new ArrayList();
		else if (!_type.isInterface()) {
			try {
				list = (Collection) _type.newInstance();
			} catch (Exception e) {
			}
		}

		if (list != null) {
		} else if (SortedSet.class.isAssignableFrom(_type))
			list = new TreeSet();
		else if (Set.class.isAssignableFrom(_type))
			list = new HashSet();
		else if (List.class.isAssignableFrom(_type))
			list = new ArrayList();
		else if (Collection.class.isAssignableFrom(_type))
			list = new ArrayList();
		else {
			try {
				list = (Collection) _type.newInstance();
			} catch (Exception e) {
				throw new IOExceptionWrapper(e);
			}
		}

		return list;
	}
}
