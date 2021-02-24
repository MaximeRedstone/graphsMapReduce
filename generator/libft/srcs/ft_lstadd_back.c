/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstadd_back.c                                   :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 11:34:55 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/03 17:01:07 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstadd_back(t_head *head, t_lst *new)
{
	head->size++;
	new->prev = head->last;
	head->last = new;
	if (new->prev)
	{
		new->prev->next = new;
	}
	else
	{
		head->first = new;
	}
}
