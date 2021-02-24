/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_lsttransfer.c                                   :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/11/03 13:06:27 by sikpenou          #+#    #+#             */
/*   Updated: 2019/11/03 13:19:10 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

void	ft_lsttransfer(t_head *head_from, t_head *head_to, t_lst *elem)
{
	t_lst	*transfer;

	if (!(transfer = ft_lstpop(head_from, elem)))
		return ;
	ft_lstadd(head_to, transfer);
}
