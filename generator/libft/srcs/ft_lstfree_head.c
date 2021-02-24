/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lsthead_free.c                                  :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 11:55:15 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/06 13:47:40 by hehlinge         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lstfree_head(t_head **head)
{
	(*head)->first = NULL;
	(*head)->last = NULL;
	ft_free((void **)head);
	return ;
}
