/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lstjoin.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/10/31 11:31:04 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/06 13:45:35 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstjoin(t_head *dst, t_head *src)
{
	if (src->first)
		src->first->prev = dst->last;
	if (dst->last)
		dst->last->next = src->first;
	else
		dst->first = src->first;
	dst->size += src->size;
	dst->last = src->last;
}
