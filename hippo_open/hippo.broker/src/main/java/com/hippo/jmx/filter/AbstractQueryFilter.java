package com.hippo.jmx.filter;

import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * 
 * @author saitxuc
 * 2015-3-20
 */
public abstract class AbstractQueryFilter implements QueryFilter {
    protected QueryFilter next;

    /**
     * Creates a query filter, with the next filter specified by next.
     * @param next - the next query filter
     */
    protected AbstractQueryFilter(QueryFilter next) {
        this.next = next;
    }

    /**
     * Performs a query given the query string
     * @param query - query string
     * @return objects that matches the query
     * @throws Exception
     */
    public List query(String query) throws Exception {
        // Converts string query to map query
        StringTokenizer tokens = new StringTokenizer(query, QUERY_DELIMETER);
        return query(Collections.list(tokens));
    }

}
