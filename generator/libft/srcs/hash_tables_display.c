/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   hash_tables_display.c                              :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 14:47:51 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:48:12 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	hash_print_elem(t_h_elem *elem)
{
	ft_printf("printing hash elem %p\nkey    : %s\nhash   : %u, content: %p\n"
			, elem, elem->key, elem->hash, elem->content);
}

void	hash_print_index(t_h_table *table, unsigned index, int opt)
{
	t_head	*index_head;
	t_lst	*hash_elem_lst;

	index_head = &(table->array[index]);
	hash_elem_lst = index_head->first;
	if (hash_elem_lst)
	{
		ft_printf("table %p, index %u (%p):\n", table, index, index_head);
		while (hash_elem_lst)
		{
			hash_print_elem(hash_elem_lst->content);
			hash_elem_lst = hash_elem_lst->next;
		}
		ft_printf("\n");
	}
	else if (opt == VERBOSE)
	{
		ft_printf("table %p, index %u (%p): index empty\n", table, index,
			index_head);
		ft_printf("\n");
	}
}

void	hash_print_table(t_h_table *table, int opt)
{
	unsigned	index;

	index = 0;
	ft_printf("printing hash table %p\nsize : %u\nelems: %u\ncoll : %u\n"
		"limit: %u\n", table, table->size, table->elems, table->collisions
		, table->collisions_limit);
	while (index < table->size)
	{
		hash_print_index(table, index, opt);
		index++;
	}
}
