/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstpop.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/24 15:06:05 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/14 13:36:28 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	isolate_lst_elem(t_lst *elem)
{
	elem->next = NULL;
	elem->prev = NULL;
}

void	pop_lst_elem(t_head *head, t_lst *elem)
{
	if (head->first == elem)
		head->first = elem->next;
	else
		elem->prev->next = elem->next;
	if (head->last == elem)
		head->last = elem->prev;
	else
		elem->next->prev = elem->prev;
}

t_lst	*ft_lstpop(t_head *head, void *match)
{
	t_lst	*elem;

	elem = ft_lstfind(head, match);
	if (!elem)
		return (0);
	head->size--;
	pop_lst_elem(head, elem);
	isolate_lst_elem(elem);
	return (elem);
}
