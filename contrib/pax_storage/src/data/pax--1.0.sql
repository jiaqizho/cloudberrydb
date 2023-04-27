/* contrib/pax_storage/src/data/pax--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pax" to load this file. \quit

CREATE FUNCTION pg_catalog.pax_tableam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'pax_tableam_handler'
LANGUAGE C STABLE STRICT;

CREATE ACCESS METHOD pax TYPE table HANDLER pg_catalog.pax_tableam_handler;
