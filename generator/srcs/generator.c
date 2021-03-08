#include "graph_generator.h"
#include <stdlib.h>
#include <fcntl.h>
#include <stdio.h>
#include <limits.h>
#include <float.h>
#include <unistd.h>

long double               get_min_range(t_generator *generator)
{
    long double   ret;

    ret = ((long double)(1 - ERROR_MARGIN) * (long double)generator->edges_per_node);
    // printf("min_range = %Lf\n", ret);
    return (ret);
}

long double             get_max_range(t_generator *generator)
{
    long double   ret;

    ret = ((long double)(1 + ERROR_MARGIN) * (long double)generator->edges_per_node);
    // printf("max_range = %Lf\n", ret);
    return (ret);
}

long double   get_avg_len_node(long double nb_nodes)
{
    int                 len;
    long double         min_number_len;
    long double         total_of_len;

    // printf("In get_avg_len_node with nb_nodes %Lf\n", nb_nodes);
    if (nb_nodes <= 9.0)
        return nb_nodes;
    len = ft_strlen(ft_itoa(nb_nodes));
    min_number_len = ft_pow(10, len - 1);
    total_of_len = len * (nb_nodes - min_number_len + 1.0);
    return total_of_len + get_avg_len_node(min_number_len - 1.0);
}

long double    get_avg_len_line(unsigned long long nb_nodes)
{
    long double     avg_len_node;
    long double     avg_len_size;
    
    avg_len_node = get_avg_len_node(nb_nodes) / nb_nodes;
    avg_len_size = 2 + 2 * avg_len_node;
    // printf("Average length of line %Lf\n", avg_len_size);
    return (avg_len_size);
}

void    get_nb_edges(t_generator *generator)
{
    long double   avg_len_line;
    long double   nb_edges;
    long double   nb_nodes;
    long double   multiplier;
    long double   edges_per_node;

    nb_nodes = 2.0;
    edges_per_node = FLT_MAX;
    while ((edges_per_node <= get_min_range(generator)) || (edges_per_node >= get_max_range(generator)))
    {
        avg_len_line = get_avg_len_line(nb_nodes);
        // printf("avg_len_line = %Lf\n", avg_len_line);
        nb_edges = generator->size / avg_len_line;
        // printf("nb_edges = %Lf\n", nb_edges);
        edges_per_node = nb_edges / nb_nodes;
        // printf("edges_per_node = %Lf\n", edges_per_node);
        multiplier = edges_per_node / generator->edges_per_node;
        // printf("multiplier = %Lf\n", multiplier);
        nb_nodes = nb_nodes * multiplier;
        // printf("Nb nodes = %Lf\n\n", nb_nodes);
    }
    generator->nb_nodes = (unsigned long long)nb_nodes;
    generator->approximate_edges = (unsigned long long)nb_edges;
    printf("Final number of nodes is %llu\n", generator->nb_nodes);
    printf("Final number of edges approximation is %llu\n", generator->approximate_edges);
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