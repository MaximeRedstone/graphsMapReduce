/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstcycle.c                                      :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/09/30 20:13:12 by sikpenou          #+#    #+#             */
/*   Updated: 2019/09/30 20:22:53 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

int		ft_lstcycle(t_lst *begin)
{
	t_lst	*slow;
	t_lst	*fast;

	if (!begin)
		return (0);
	slow = begin;
	fast = begin;
	while (fast)
	{
		if (fast->next == slow)
			return (1);
		fast = fast->next;
		if (!fast)
			return (0);
		if (fast->next == slow)
			return (1);
		fast = fast->next;
		slow = slow->next;
	}
	return (0);
}
