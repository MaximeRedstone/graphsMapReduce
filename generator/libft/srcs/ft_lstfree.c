/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstclr.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/08 18:03:51 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/11 12:24:17 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstfree(t_head **head, int opt_elems, int opt_head)
{
	t_lst	*elem;

	if (*head)
	{
		if ((elem = (*head)->first))
		{
			while (elem->next)
			{
				elem = elem->next;
				ft_lstfree_elem(&(elem->prev), opt_elems);
			}
			ft_lstfree_elem(&(elem), opt_elems);
		}
		if (opt_head == FREE_HEAD)
			ft_lstfree_head(head);
		else
		{
			(*head)->first = 0;
			(*head)->last = 0;
			(*head)->size = 0;
		}
	}
	return ;
}
