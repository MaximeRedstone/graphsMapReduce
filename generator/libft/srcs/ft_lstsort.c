/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstsort.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 13:01:46 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/13 15:16:30 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstsort(t_head *head, int (*cmp)())
{
	t_lst		*elem;

	if (!head || !head->first || !cmp)
		return ;
	elem = head->first;
	while (elem->next)
	{
		if ((*cmp)(elem->content, (elem->next)->content) > 0)
		{
			ft_lstswap_contents(elem, elem->next);
		}
		else
			elem = elem->next;
	}
}
