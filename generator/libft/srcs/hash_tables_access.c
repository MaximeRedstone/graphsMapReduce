/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   hash_tables_access.c                               :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 14:47:21 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/11 18:07:55 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int			ft_hash_iter(t_h_table *table, t_h_iter_func iter_func)
{
	unsigned	index;
	t_lst		*index_lst;

	index = 0;
	while (index < table->size)
	{
		if (table->array[index].first)
		{
			index_lst = table->array[index].first;
			while (index_lst)
			{
				if (!(iter_func(index_lst->content)))
					return (0);
				index_lst = index_lst->next;
			}
		}
		index++;
	}
	return (1);
}

t_h_elem	*ft_hash_get_elem(t_h_table *table, char *key)
{
	unsigned	index;
	t_h_elem	*hash_elem;
	t_lst		*index_lst;

	index = table->hash_func(key) % table->size;
	index_lst = table->array[index].first;
	while (index_lst)
	{
		hash_elem = index_lst->content;
		if (!ft_strcmp(hash_elem->key, key))
			return (hash_elem);
		index_lst = index_lst->next;
	}
	return (NULL);
}

void		*ft_hash_get_content(t_h_table *table, char *key)
{
	t_h_elem	*hash_elem;

	hash_elem = ft_hash_get_elem(table, key);
	if (hash_elem)
		return (hash_elem->content);
	return (NULL);
}

t_h_elem	*ft_hash_pop_elem(t_h_table *table, char *key)
{
	unsigned	index;
	t_head		*index_head;
	t_lst		*popped_lst;
	t_h_elem	*popped_elem;

	index = table->hash_func(key) % table->size;
	index_head = &(table->array[index]);
	if (index_head)
	{
		popped_elem = ft_hash_get_elem(table, key);
		popped_lst = ft_lstpop(index_head, popped_elem);
		if (popped_lst)
		{
			if (index_head->size > 0)
				table->collisions--;
			ft_lstfree_elem(&popped_lst, FREE_LINKS);
			table->elems--;
			return (popped_elem);
		}
	}
	return (NULL);
}
