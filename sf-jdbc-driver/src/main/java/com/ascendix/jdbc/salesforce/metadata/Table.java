package com.ascendix.jdbc.salesforce.metadata;

import java.io.Serializable;
import java.util.List;

public class Table implements Serializable {

    private String name;
    private String comments;
    private List<Column> columns;
    private boolean queryable;

    public Table(String name, String comments, List<Column> columns, boolean queryable) {
        this.name = name;
        this.comments = comments;
        this.columns = columns;
        this.queryable = queryable;
        for (Column c : columns) {
            c.setTable(this);
        }
    }

    public String getName() {
        return name;
    }

    public String getComments() {
        return comments;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public boolean isQueryable () {
        return queryable;
    }

    public Column findColumn(String columnName) {
        return columns.stream()
                .filter(column -> columnName.equals(column.getName()))
                .findFirst()
                .orElse(null);
    }

}
