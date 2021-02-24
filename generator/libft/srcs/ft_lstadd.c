/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstadd.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/08 18:03:41 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/02 18:29:06 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstadd(t_head *head, t_lst *new_link)
{
	head->size++;
	new_link->next = head->first;
	head->first = new_link;
	if (new_link->next)
	{
		new_link->next->prev = new_link;
	}
	else
	{
		head->last = new_link;
	}
}
