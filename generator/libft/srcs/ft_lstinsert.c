/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstinsert.c                                     :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/12/14 12:57:17 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/14 13:57:35 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	insert_before(t_head *head, t_lst *ref, t_lst *insert)
{
	insert->prev = ref->prev;
	insert->next = ref;
	if (ref != head->first)
		ref->prev->next = insert;
	else
		head->first = insert;
	ref->prev = insert;
}

void	insert_after(t_head *head, t_lst *ref, t_lst *insert)
{
	insert->next = ref->next;
	insert->prev = ref;
	if (ref != head->last)
		ref->next->prev = insert;
	else
		head->last = insert;
	ref->next = insert;
}

void	ft_lstinsert(t_head *head, t_lst *ref, t_lst *insert, int bef_or_aft)
{
	head->size++;
	if (bef_or_aft == BEFORE)
		insert_before(head, ref, insert);
	else
		insert_after(head, ref, insert);
}
