#include "graph_generator.h"
#include <unistd.h>
#include <stdlib.h>

void    write_to_buffer(char *to_write, char *buffer, unsigned long long *buffer_counter, t_generator *generator)
{
    int     pos;

    pos = -1;
    while (to_write[++pos])
    {
        buffer[*buffer_counter] = to_write[pos];
        *buffer_counter += 1;
        generator->already_written += 1;
        if (*buffer_counter == BUF_SIZE)
        {
            write(generator->fd, buffer, *buffer_counter);
            *buffer_counter = 0;
        }
    }
}

void    generate_edge(char *buffer, unsigned long long *buffer_counter, t_generator *generator)
{
    unsigned long long  edge;
    unsigned long long  edge2;
    char                *edge_str;

    edge = rand() % generator->nb_nodes + 1;
    edge_str = ft_itoa(edge);
    write_to_buffer(edge_str, buffer, buffer_counter, generator);
    write_to_buffer("\t", buffer, buffer_counter, generator);
    free(edge_str);
    edge2 = rand() % generator->nb_nodes + 1;
    while (edge2 == edge)
        edge2 = rand() % generator->nb_nodes + 1;
    edge_str = ft_itoa(edge2);
    write_to_buffer(edge_str, buffer, buffer_counter, generator);
    write_to_buffer("\n", buffer, buffer_counter, generator);
    free(edge_str);
}