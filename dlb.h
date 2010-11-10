#if defined __STDC__ && __STDC__
# define __CONCAT(x,y) x ## y
# define __STRING(x) #x
#else
# define __CONCAT(x,y) x/**/y
# define __STRING(x) "x"
#endif

# define ASSERT(expr) if(.not.(expr))call DLB_ASSERT_FAILED(__STRING(expr),__FILE__,__LINE__)

/* FIXME: must die, use ASSERT(expr) instead: */
# define assert_n(expr, code) dlb_assert_failed(__STRING(expr),__FILE__,__LINE__)
