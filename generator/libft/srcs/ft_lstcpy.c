/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstcpy.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 12:22:43 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/03 10:53:00 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_head	*ft_lstcpy(t_head *original_head)
{
	t_head	*new_head;
	t_lst	*elem;

	if (!(new_head = ft_lstnew_head(NULL, NULL)))
		return (0);
	elem = original_head->last;
	while (elem)
	{
		if (!(ft_lstadd_new(new_head, elem->content)))
			return (0);
		elem = elem->prev;
	}
	return (new_head);
}
