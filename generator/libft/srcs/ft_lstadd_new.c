/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstadd_new.c                                    :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/08/03 12:40:21 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/13 18:27:59 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_lst	*ft_lstadd_new(t_head *head, void *content)
{
	t_lst	*new_elem;

	if (!(new_elem = ft_lstnew_elem(content)))
	{
		return (0);
	}
	else
	{
		ft_lstadd(head, new_elem);
	}
	return (new_elem);
}
