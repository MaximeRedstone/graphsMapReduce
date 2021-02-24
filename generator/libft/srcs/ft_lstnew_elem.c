/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstnew.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/08 18:05:42 by sikpenou          #+#    #+#             */
/*   Updated: 2019/12/13 18:30:31 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

t_lst	*ft_lstnew_elem(void *content)
{
	t_lst	*new_elem;

	if (!(new_elem = (t_lst *)easymalloc(sizeof(*new_elem))))
		return (NULL);
	ft_memset(new_elem, 0, sizeof(*new_elem));
	new_elem->content = content;
	return (new_elem);
}
