#ifndef LEM_IN_H
# define LEM_IN_H

# include "libft.h"
# include <time.h>

# define WRITE_HEADER 1

# define INCORRECT_NB_ARGUMENTS 1
# define UNRECOGNIZED_UNIT 2
# define FD_ERROR 3
# define MALLOC_ERROR 4

# define UNITS "BKMG"

# define BUF_SIZE 0x2000000
# define ERROR_MARGIN 0.1

typedef struct		        s_generator
{
    int                     fd;
	char    		        unit;
	unsigned long long      user_input;
	unsigned long long	    size;
    unsigned long long      already_written;
    unsigned long long      nb_nodes;
    long double             edges_per_node;
    unsigned long long      approximate_edges;
    time_t                  start;
}					        t_generator;

int     parse_user_args(int argc, char **argv, t_generator *generator);
int     convert_user_input(t_generator *generator);
int     generate_graph(char *filename, t_generator *generator);
int     exit_failure(int error_code);
void    generate_edge(char *buffer, unsigned long long *buffer_counter, t_generator *generator);
void    write_header(t_generator *generator);

#endif
