/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstnew.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 12:22:43 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/03 12:25:27 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_head	*ft_lstnew(void *content)
{
	t_lst	*new_elem;
	t_head	*new_head;

	if (!(new_elem = ft_lstnew_elem(content)))
		return (0);
	if (!(new_head = ft_lstnew_head(new_elem, new_elem)))
		return (0);
	return (new_head);
}
