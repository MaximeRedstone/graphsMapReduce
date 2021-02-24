#include "graph_generator.h"
#include <stdlib.h>

// # include <stdio.h>

int exit_failure(int error_code)
{
    if (error_code == INCORRECT_NB_ARGUMENTS)
        ft_putstr("Invalid number of user argument. 3 argument are required\n");
    else if (error_code == UNRECOGNIZED_UNIT)
        ft_putstr("Unrecognized unit. [B, K, M, G] allowed\n");
    else if (error_code == FD_ERROR)
        ft_putstr("Unable to open file\n");
    else if (error_code == MALLOC_ERROR)
        ft_putstr("Unable to alloc memory of predefined buffer size");
    return (EXIT_SUCCESS);
}