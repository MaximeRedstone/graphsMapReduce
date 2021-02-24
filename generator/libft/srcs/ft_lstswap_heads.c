/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstswap_heads.c                                 :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/12/13 11:24:59 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 14:46:16 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	update_head_first(t_head *head, t_lst *elem_1, t_lst *elem_2)
{
	if (head->first == elem_1)
		head->first = elem_2;
	else if (head->first == elem_2)
		head->first = elem_1;
}

void	update_head_last(t_head *head, t_lst *elem_1, t_lst *elem_2)
{
	if (head->last == elem_1)
		head->last = elem_2;
	else if (head->last == elem_2)
		head->last = elem_1;
}

void	swap_prev(t_lst *elem_1, t_lst *elem_2)
{
	t_lst	*tmp_prev;

	tmp_prev = elem_1->prev;
	elem_1->prev = elem_2->prev;
	elem_2->prev = tmp_prev;
}

void	swap_next(t_lst *elem_1, t_lst *elem_2)
{
	t_lst	*tmp_next;

	tmp_next = elem_1->next;
	elem_1->next = elem_2->next;
	elem_2->next = tmp_next;
}

void	ft_lstswap_heads(t_head *head, t_lst *elem_1, t_lst *elem_2)
{
	update_head_first(head, elem_1, elem_2);
	update_head_last(head, elem_1, elem_2);
	swap_prev(elem_1, elem_2);
	swap_next(elem_1, elem_2);
}
