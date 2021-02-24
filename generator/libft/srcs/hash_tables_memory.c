/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   hash_tables_memory.c                               :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 14:48:39 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/06 10:50:26 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void			ft_hash_free_elem(t_h_table *table, t_h_elem *hash_elem
	, int opt)
{
	if (opt != FREE_LINKS)
		table->free_func(&(hash_elem->content));
	easyfree((void **)&hash_elem);
}

void			ft_hash_free_table(t_h_table *table, int opt)
{
	unsigned	index;
	t_head		*index_head;
	t_lst		*index_lst;

	index = 0;
	while (index < table->size)
	{
		if (table->array[index].first)
		{
			index_head = &(table->array[index]);
			while (index_head->first)
			{
				index_lst = ft_lstpop(index_head, index_head->first->content);
				ft_hash_free_elem(table, index_lst->content, opt);
				ft_lstfree_elem(&index_lst, FREE_LINKS);
			}
		}
		index++;
	}
	easyfree((void **)&table->array);
	easyfree((void **)&table);
}

static unsigned	hash_str_key(char *key)
{
	unsigned	hash;

	hash = 0;
	while (*key)
	{
		hash = hash * 97 + *key;
		key++;
	}
	return (hash);
}

void			basic_free_hash_elem_content(void **content)
{
	easyfree((void **)content);
}

t_h_table		*ft_hash_new_table(void)
{
	t_h_table	*new_table;
	unsigned	size;

	if (!(new_table = (t_h_table *)easymalloc(sizeof(*new_table))))
		return (0);
	new_table->hash_func = &hash_str_key;
	new_table->free_func = &basic_free_hash_elem_content;
	size = ft_find_next_prime(INITIAL_HASH_ARRAY_SIZE);
	if (!ft_hash_resize_array(new_table, size))
		return (NULL);
	return (new_table);
}
