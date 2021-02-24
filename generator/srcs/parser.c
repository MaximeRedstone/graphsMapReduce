# include "graph_generator.h"
# include <stdlib.h>
# include <stdio.h>
# include "libft.h"

int    convert_user_input(t_generator *generator)
{
    char    *unit;
    int     pos;

    if (!(unit = ft_strchr(UNITS, (int)generator->unit)))
        return UNRECOGNIZED_UNIT;
    pos = unit - (char *)(&UNITS);
    generator->size = (1 << (pos * 10)) * generator->user_input;
    return EXIT_SUCCESS;
}

int     parse_user_args(int argc, char **argv, t_generator *generator)
{
    int     strlen;

    if (argc != 4)
        return INCORRECT_NB_ARGUMENTS;
    strlen = ft_strlen(argv[1]);
    generator->unit = argv[1][strlen - 1];
    argv[1][strlen - 1] = '\0';
	generator->user_input = ft_atoi(argv[1]);
    generator->edges_per_node = (float)ft_atoi(argv[3]);
    return (EXIT_SUCCESS);
}