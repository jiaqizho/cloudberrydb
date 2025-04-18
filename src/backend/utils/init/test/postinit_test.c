/*-------------------------------------------------------------------------
*
* postinit_test.c
*
*--------------------------------------------------------------------------
*/
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

/* Fetch definition of PG_exception_stack */
#include "postgres.h"

#undef PG_RE_THROW
#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

static void
_errfinish_impl()
{
	PG_RE_THROW();
}

static void expect_ereport(int expect_elevel)
{
	if (expect_elevel < ERROR)
	{
		expect_value(errstart, elevel, expect_elevel);
		expect_any(errstart, domain);
		will_return(errstart, false);
	}
	else
	{
		expect_value(errstart_cold, elevel, expect_elevel);
		expect_any(errstart_cold, domain);
		will_return_with_sideeffect(errstart_cold, false, &_errfinish_impl, NULL);
	}
}

#include "../postinit.c"

static void
test_check_superuser_connection_limit_error(void **state)
{
	am_ftshandler = false;

	expect_value(HaveNFreeProcs, n, RESERVED_FTS_CONNECTIONS);
	will_return(HaveNFreeProcs, false);

	expect_ereport(FATAL);

	/*
	 * Expect ERROR
	 */
	PG_TRY();
	{
		check_superuser_connection_limit();
		fail();
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();
}

static void
test_check_superuser_connection_limit_ok_with_free_procs(void **state)
{
	am_ftshandler = false;

	expect_value(HaveNFreeProcs, n, RESERVED_FTS_CONNECTIONS);
	will_return(HaveNFreeProcs, true);

	/*
	 * Expect OK
	 */
	check_superuser_connection_limit();
}

static void
test_check_superuser_connection_limit_ok_for_ftshandler(void **state)
{
	am_ftshandler = true;

	/*
	 * Expect OK
	 */
	check_superuser_connection_limit();
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_check_superuser_connection_limit_ok_with_free_procs),
		unit_test(test_check_superuser_connection_limit_ok_for_ftshandler),
		unit_test(test_check_superuser_connection_limit_error),
	};

	return run_tests(tests);
}
