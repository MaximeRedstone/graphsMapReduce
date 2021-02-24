#include "graph_generator.h"
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>

void    write_header(t_generator *generator)
{
    dprintf(generator->fd, "# Launched at %s", ctime(&generator->start));
    dprintf(generator->fd, "# User input was: requested size = %llu%c, nb_edges_per_node = %d\n",
        generator->user_input, generator->unit, (int)generator->edges_per_node);
    dprintf(generator->fd, "# Number of nodes is %llu\n", generator->nb_nodes);
    dprintf(generator->fd, "# Expected number of edges is %llu\n", generator->approximate_edges);   
}