/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstadd_sorted.c                                 :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/03 14:45:14 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:45:16 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	add_in_middle(t_head *head, t_lst *new_link
	, int (*cmp)(t_lst *, t_lst *))
{
	t_lst	*elem;

	elem = head->first;
	while (elem)
	{
		if ((*cmp)(elem, new_link) == 1)
		{
			head->size++;
			new_link->prev = elem->prev;
			new_link->next = elem;
			elem->prev->next = new_link;
			elem->prev = new_link;
			return ;
		}
		elem = elem->next;
	}
	return ;
}

void	ft_lstadd_sorted(t_head *head, t_lst *new_link
	, int (*cmp)(t_lst *, t_lst *))
{
	if (!head->first || (*cmp)(head->first, new_link) == 1)
	{
		ft_lstadd(head, new_link);
	}
	else if ((*cmp)(head->last, new_link) == 0)
	{
		ft_lstadd_back(head, new_link);
	}
	else
	{
		add_in_middle(head, new_link, (*cmp));
	}
}
