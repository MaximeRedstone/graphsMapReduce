/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstadd_back_new.c                               :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2020/01/13 10:33:18 by sikpenou          #+#    #+#             */
/*   Updated: 2020/01/13 10:33:39 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_lst	*ft_lstadd_back_new(t_head *head, void *content)
{
	t_lst	*new_elem;

	if (!(new_elem = ft_lstnew_elem(content)))
	{
		return (0);
	}
	else
	{
		ft_lstadd_back(head, new_elem);
	}
	return (new_elem);
}
