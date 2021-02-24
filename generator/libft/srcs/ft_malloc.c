/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   my_malloc.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/07/24 23:04:53 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:46:58 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"
#include <stdlib.h>

t_head		*get_gc_list(int opt)
{
	static t_head	*gc_list = NULL;

	if (!gc_list && opt == 1)
	{
		if (!(gc_list = (t_head *)malloc(sizeof(*gc_list))))
			return (0);
		ft_memset(gc_list, 0, sizeof(*gc_list));
	}
	else if (opt == -1)
		gc_list = NULL;
	return (gc_list);
}

void		easyfree_gc(void)
{
	t_lst	*elem;
	t_lst	*tmp;
	t_head	*gc_list;

	if (!(gc_list = get_gc_list(0)))
		return ;
	elem = (gc_list)->first;
	while (elem)
	{
		gc_list->size--;
		tmp = elem;
		elem = elem->next;
		if (tmp->content)
			free(tmp->content);
		free(tmp);
	}
	free(gc_list);
	get_gc_list(-1);
}

void		easyfree(void **match)
{
	t_lst	*elem;
	t_head	*gc_list;

	if (EASY)
	{
		if (!*match || !(gc_list = get_gc_list(0)))
			return ;
		if (!(elem = ft_lstpop(gc_list, *match)))
			return ;
		free(elem);
		elem = NULL;
	}
	if (*match)
		free(*match);
	*match = NULL;
}

void		*easymalloc(size_t size)
{
	t_lst	*new_free;
	t_head	*gc_list;

	if (!EASY)
	{
		return (ft_memalloc(size));
	}
	if (size < 1 || !(gc_list = get_gc_list(1)))
		return (0);
	if (!(new_free = (t_lst *)malloc(sizeof(t_lst))))
		return (0);
	ft_lstadd(gc_list, new_free);
	if (!(new_free->content = ft_memalloc(size)))
		return (0);
	return (new_free->content);
}
