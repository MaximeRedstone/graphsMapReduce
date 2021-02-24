/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstfind.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/30 16:07:51 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/13 11:40:20 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_lst	*ft_lstfind(t_head *head, void *match)
{
	t_lst	*elem;

	if (!head || !head->first)
		return (NULL);
	elem = head->first;
	while (elem)
	{
		if (elem->content == match)
			return (elem);
		elem = elem->next;
	}
	return (NULL);
}
