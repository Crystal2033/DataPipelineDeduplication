/*
 * This file is generated by jOOQ.
 */
package ru.mai.lessons.rpks.jooq.tables;


import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function6;
import org.jooq.Identity;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row6;
import org.jooq.Schema;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import ru.mai.lessons.rpks.jooq.Keys;
import ru.mai.lessons.rpks.jooq.Public;
import ru.mai.lessons.rpks.jooq.tables.records.DeduplicationRulesRecord;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DeduplicationRules extends TableImpl<DeduplicationRulesRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.deduplication_rules</code>
     */
    public static final DeduplicationRules DEDUPLICATION_RULES = new DeduplicationRules();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<DeduplicationRulesRecord> getRecordType() {
        return DeduplicationRulesRecord.class;
    }

    /**
     * The column <code>public.deduplication_rules.id</code>.
     */
    public final TableField<DeduplicationRulesRecord, Integer> ID = createField(DSL.name("id"), SQLDataType.INTEGER.nullable(false).identity(true), this, "");

    /**
     * The column <code>public.deduplication_rules.deduplication_id</code>.
     */
    public final TableField<DeduplicationRulesRecord, Long> DEDUPLICATION_ID = createField(DSL.name("deduplication_id"), SQLDataType.BIGINT.nullable(false), this, "");

    /**
     * The column <code>public.deduplication_rules.rule_id</code>.
     */
    public final TableField<DeduplicationRulesRecord, Long> RULE_ID = createField(DSL.name("rule_id"), SQLDataType.BIGINT.nullable(false), this, "");

    /**
     * The column <code>public.deduplication_rules.field_name</code>.
     */
    public final TableField<DeduplicationRulesRecord, String> FIELD_NAME = createField(DSL.name("field_name"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.deduplication_rules.time_to_live_sec</code>.
     */
    public final TableField<DeduplicationRulesRecord, Long> TIME_TO_LIVE_SEC = createField(DSL.name("time_to_live_sec"), SQLDataType.BIGINT.nullable(false), this, "");

    /**
     * The column <code>public.deduplication_rules.is_active</code>.
     */
    public final TableField<DeduplicationRulesRecord, Boolean> IS_ACTIVE = createField(DSL.name("is_active"), SQLDataType.BOOLEAN.nullable(false), this, "");

    private DeduplicationRules(Name alias, Table<DeduplicationRulesRecord> aliased) {
        this(alias, aliased, null);
    }

    private DeduplicationRules(Name alias, Table<DeduplicationRulesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.deduplication_rules</code> table reference
     */
    public DeduplicationRules(String alias) {
        this(DSL.name(alias), DEDUPLICATION_RULES);
    }

    /**
     * Create an aliased <code>public.deduplication_rules</code> table reference
     */
    public DeduplicationRules(Name alias) {
        this(alias, DEDUPLICATION_RULES);
    }

    /**
     * Create a <code>public.deduplication_rules</code> table reference
     */
    public DeduplicationRules() {
        this(DSL.name("deduplication_rules"), null);
    }

    public <O extends Record> DeduplicationRules(Table<O> child, ForeignKey<O, DeduplicationRulesRecord> key) {
        super(child, key, DEDUPLICATION_RULES);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public Identity<DeduplicationRulesRecord, Integer> getIdentity() {
        return (Identity<DeduplicationRulesRecord, Integer>) super.getIdentity();
    }

    @Override
    public UniqueKey<DeduplicationRulesRecord> getPrimaryKey() {
        return Keys.DEDUPLICATION_RULES_PKEY;
    }

    @Override
    public DeduplicationRules as(String alias) {
        return new DeduplicationRules(DSL.name(alias), this);
    }

    @Override
    public DeduplicationRules as(Name alias) {
        return new DeduplicationRules(alias, this);
    }

    @Override
    public DeduplicationRules as(Table<?> alias) {
        return new DeduplicationRules(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public DeduplicationRules rename(String name) {
        return new DeduplicationRules(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public DeduplicationRules rename(Name name) {
        return new DeduplicationRules(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public DeduplicationRules rename(Table<?> name) {
        return new DeduplicationRules(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row6 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row6<Integer, Long, Long, String, Long, Boolean> fieldsRow() {
        return (Row6) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function6<? super Integer, ? super Long, ? super Long, ? super String, ? super Long, ? super Boolean, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function6<? super Integer, ? super Long, ? super Long, ? super String, ? super Long, ? super Boolean, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
