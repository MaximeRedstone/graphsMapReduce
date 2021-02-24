#include "graph_generator.h"
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <limits.h>
#include <unistd.h>

unsigned long long      get_min_range(t_generator *generator)
{
    return (unsigned long long)((long double)(1 - ERROR_MARGIN) * (long double)generator->edges_per_node);
}

unsigned long long      get_max_range(t_generator *generator)
{
    return (unsigned long long)((long double)(1 + ERROR_MARGIN) * (long double)generator->edges_per_node);
}

unsigned long long get_avg_len_node(unsigned long long nb_nodes)
{
    int                 len;
    unsigned long long  min_number_len;
    unsigned long long  total_of_len;

    if (nb_nodes <= 9LL)
        return nb_nodes;
    len = ft_strlen(ft_itoa(nb_nodes));
    min_number_len = ft_pow(10, len - 1);
    total_of_len = len * (nb_nodes - min_number_len + 1);
    return total_of_len + get_avg_len_node(min_number_len - 1);
}

unsigned long long get_avg_len_line(unsigned long long nb_nodes)
{
    long double     avg_len_node;
    long double     avg_len_size;
    
    avg_len_node = get_avg_len_node(nb_nodes) / nb_nodes;
    avg_len_size = 2 + 2 * avg_len_node;
    return (unsigned long long)(avg_len_size);
}

void    get_nb_edges(t_generator *generator)
{
    unsigned long long  avg_len_line;
    unsigned long long  nb_edges;
    unsigned long long  nb_nodes;
    float               multiplier;
    float               edges_per_node;

    nb_nodes = 2;
    edges_per_node = ULLONG_MAX;
    while ((edges_per_node < get_min_range(generator)) || (edges_per_node > get_max_range(generator)))
    {
        avg_len_line = get_avg_len_line(nb_nodes);
        nb_edges = generator->size / avg_len_line;
        edges_per_node = (float)nb_edges / (float)nb_nodes;
        multiplier = (float)edges_per_node / generator->edges_per_node;
        nb_nodes = nb_nodes * multiplier;
    }
    generator->nb_nodes = nb_nodes;
    generator->approximate_edges = nb_edges;
}

int     generate_graph(char *filename, t_generator *generator)
{
    unsigned long long  buffer_counter;
    char                *buffer;

    buffer_counter = 0LL;
    buffer = (char *)malloc(sizeof(char) * BUF_SIZE);
    if (!buffer)
        return MALLOC_ERROR;    
    get_nb_edges(generator);
    generator->fd = open(filename, O_WRONLY | O_CREAT, 0644);
	if (generator->fd < 3)
		return (exit_failure(FD_ERROR));
    write_header(generator);
    while (generator->already_written < generator->size)
        generate_edge(buffer, &buffer_counter, generator);
    write(generator->fd, buffer, buffer_counter);
    close(generator->fd);
    free(buffer);
    return (EXIT_SUCCESS);
}